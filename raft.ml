open Batteries
open Raft_intf

module L = Log.Debug

type term_id = int

let election_timeout = 0.225
let election_timeout_spray = 0.333
let heartbeat_timeout = 0.1
let nodelay_replication = false (* true for replicating each command as they are received *)
let random_election_timeout () =
    let s = election_timeout_spray in
    let r = Random.float (s*.2.) -. s +. 1. in
    assert (r > 0.) ;
    election_timeout *. r

let rec until_diff x f =
    let y = f () in
    if x <> y then y else until_diff x f

module type COMMAND = sig
    type t (* the type of a command for the state machine *)
    val print : 'a BatInnerIO.output -> t -> unit
    type state (* the type of the value of the state machine, aka apply return value *)
end

(* We build both client and server from the same RPC_Maker and COMMAND so that type
 * correctness is enforced (a bit more) *)
module Make (RPC_Maker : RPC.Maker) (Command : COMMAND) =
struct

    type peer_info = { mutable next_index : int (* index of the next log entry to send there *) ;
                       mutable match_index : int (* index of highest log entry known to be replicated there *) ;
                       mutable last_sent : float (* when we last sent entries there *) ;
                       mutable in_flight : bool (* do we have a previous append_entries going on? *) }

    type leader_t = (Host.t, peer_info) Hashtbl.t
    type state = Follower | Candidate | Leader of leader_t

    let string_of_state = function
        | Follower -> Log.colored false 4 "Follower"
        | Candidate -> Log.colored true 4 "Candidate"
        | Leader _ -> Log.colored true 7 "Leader"

    type server_info =
        { mutable state : state (* The state I'm currently in *) ;
          host : Host.t (* myself *) ;
          peers : Host.t list (* other servers *) ;
          mutable current_term : term_id (* latest term server has seen (initialized to 0 at startup, increases monotonically) *) ;
          mutable voted_for : Host.t option (* candidate that received vote in current term (if any) *) ;
          mutable commit_index : int (* index of highest log entry known to be committed *) ;
          mutable last_applied : int (* last index we applied *) ;
          mutable last_leader : Host.t (* last leader we heard from *) ;
          mutable last_recvd_from_leader : float (* when we last heard from a leader *) ;
          mutable election_timeout : float }

    (* Same RPC_ClientServers for both Server and Client *)
    module RPC_ClientServer_Types =
    struct
        type arg = ChangeState of Command.t
                 | QueryInfo (* to a specific host *)
        type ret = State of int * Command.state (* to a ChangeState only *)
                 | Redirect of Host.t (* to a ChangeState only *)
                 | Info of server_info (* to a QueryInfo only *)
    end
    module RPC_ClientServers = RPC_Maker (RPC_ClientServer_Types)

    type log_entry =
        { command : Command.t (* command for the state machine *) ;
          term : term_id (* when it was received *) ;
          write : RPC_ClientServer_Types.ret -> unit (* the function to answer to client when it's applied *) }
    let log_entry_print fmt entry =
        Printf.fprintf fmt "{command=%a, term=%d}" Command.print entry.command entry.term

    (* Same without the write function that we don't send to replicas *)
    type append_entry =
        { append_command : Command.t ;
          append_term : term_id }

    (* We store logs outside because we don't want to return those when asked for internal infos
     * (to be honest, that's because we can't marshal them) *)
    type server =
        { info : server_info ;
          mutable logs : log_entry DynArray.t }

    module Server =
    struct
        type t = server
        let print fmt t =
            Printf.fprintf fmt "%20s@%s, term=%d" (string_of_state t.info.state) (Host.to_string t.info.host) t.info.current_term

        module RPC_Server_Types =
        struct
            (* There is only two RPCs *)
            module RequestVote =
            struct
                type arg =
                    { term : term_id (* candidate's term *) ;
                      candidate_id : Host.t (* candidate requesting vote *) ;
                      last_log_index : int (* index of candidate's last log entry *) ;
                      last_log_term : term_id (* term of candidate's last log entry *) }
            end
            module AppendEntries =
            struct
                type arg =
                    { term : term_id (* leader's term *) ;
                      leader_id : Host.t (* so follower can redirect clients *) ;
                      prev_log_index : int (* index of log entry immediately preceding new ones (FIXME: what if the log was empty?) *) ;
                      prev_log_term : term_id (* term of prev_log_index entry (FIXME: idem) *) ;
                      entries : append_entry array (* log entries to store *) ;
                      leader_commit : int (* leader's commit_index *) }
            end
            type arg = RequestVote of RequestVote.arg
                     | AppendEntries of AppendEntries.arg

            type ret = 
                { term : term_id (* current term, for caller to update its state *) ;
                  success : bool (* did candidate received the vote? /
                                        follower already contained entry matching prev_log_{index,term} *) }
        end
        module RPC_Servers = RPC_Maker (RPC_Server_Types)

        let print_arg fmt = function
            | RPC_Server_Types.RequestVote rv ->
                Printf.fprintf fmt "RequestVote(term=%d,last_log_index=%d,last_log_term=%d)"
                    rv.term rv.last_log_index rv.last_log_term
            | RPC_Server_Types.AppendEntries ae ->
                let open RPC_Server_Types.AppendEntries in
                Printf.fprintf fmt "AppendEntries(nb_entries=%d@%d)" (Array.length ae.entries) (ae.prev_log_index+1)

        let init_follower host peers =
            { info = { state = Follower ;
                       election_timeout = random_election_timeout () ;
                       current_term = 0 ;
                       host ; peers ;
                       voted_for = None ;
                       commit_index = 0 ;
                       last_applied = 0 ;
                       last_leader = host ; (* no better idea *)
                       last_recvd_from_leader = Unix.gettimeofday () } ;
              logs = DynArray.create () }

        let iter_peers t f =
            List.iter f t.info.peers

        let nb_peers t = List.length t.info.peers

        (* since we are going to try many logs implementation: *)
        (* raises Invalid_argument all kind of exception if idx is bogus *)
        let log_at logs idx =
            DynArray.get logs (idx - 1 (* first index is 1 *))
        let log_at_opt logs idx =
            try Some (log_at logs idx)
            with DynArray.Invalid_arg _ -> None
        (* Return the list of logs starting at a given index *)
        let append_log_from logs idx =
            let fst = idx-1 in
            let len = DynArray.length logs - fst in
            Array.init len (fun i ->
                let entry = DynArray.get logs (fst+i) in
                { append_command = entry.command ;
                  append_term = entry.term })
        (* Append a new log entry and return last one's index *)
        let log_append logs command term write =
            let entry = { command ; term ; write } in
            DynArray.add logs entry
        let log_last_index logs =
            DynArray.length logs
        let log_truncate t n =
            t.logs <- DynArray.sub t.logs 0 n
        let log_concat logs arr =
            DynArray.append (DynArray.of_array arr) logs

        let reset_election_timeout t =
            let now = Unix.gettimeofday () in
            (*L.debug "%a: Resetting election timeout to %a" print t Log.date now ;*)
            t.info.last_recvd_from_leader <- now
            
        let convert_to_follower t =
            if t.info.state <> Follower then (
                L.debug "%a: Converting to Follower" print t ;
                t.info.state <- Follower ;
            ) ;
            t.info.election_timeout <- random_election_timeout () ;
            reset_election_timeout t

        (* Wrapper that handles common behavior *)
        let call_server t peer arg k =
            L.debug "%a: Calling %s for %a" print t (Host.to_string peer) print_arg arg ;
            RPC_Servers.call peer arg (fun res ->
                (match res with
                | Ok res ->
                    if res.term > t.info.current_term then (
                        t.info.current_term <- res.term ;
                        t.info.voted_for <- None ;
                        convert_to_follower t
                    )
                | Timeout ->
                    L.error "%a: Timeouting call %s for %a" print t (Host.to_string peer) print_arg arg
                | Err err ->
                    L.debug "%a: Received Error %s" print t err) ;
                k res)

        let answer ?dest ?reason t success =
            L.debug "%a: Answering %s%s%s"
                print t (if success then (Log.colored true 2 "YES") else (Log.colored false 1 "NO"))
                (match dest with Some d -> " to "^ Host.to_string d | None -> "")
                (match reason with Some r -> " because "^r | None -> "") ;
            { RPC_Server_Types.term = t.info.current_term ; RPC_Server_Types.success = success }

        let answer_request_vote t arg =
            let open RPC_Server_Types.RequestVote in
            let grant, reason =
                (* Reply false if term < current_term *)
                if arg.term < t.info.current_term then false, "he is from the past" else
                (* If voted_for is None or candidate_id, and candidate's log is at least as up-to-date
                 * as receiver's log, grant vote. *)
                match t.info.voted_for with Some guy when guy <> arg.candidate_id ->
                      false, "I vote for "^(Host.to_string guy)
                | _ ->
                (* Up-to-date: If the logs have last entries with different terms, then the log with
                 * the later term is more up to date. If the logs end with the same term, then whichever
                 * log is longer is more up-to-date. *)
                let last_log_index = log_last_index t.logs in
                let last_log_term =
                    if last_log_index > 0 then
                        let last_log = log_at t.logs last_log_index in
                        last_log.term
                    else 0 in
                if last_log_term > arg.last_log_term then
                    false, "his last log is too old" else
                if last_log_term < arg.last_log_term then
                    true, "his last log is from the future" else
                if last_log_index > arg.last_log_index then
                    false, "I have more logs" else
                (* TODO: I vote for a candidate with same log than me, is that correct? *)
                true, "he has more logs or same" in
            if grant then t.info.voted_for <- Some arg.candidate_id ;
            answer ~dest:arg.candidate_id ~reason t grant

        (* tells whether a majority of servers have log_idx in their log *)
        let is_committed t peers_info log_idx =
            let nb_res = ref 1 in (* at least me *)
            let has_quota () = !nb_res > (nb_peers t + 1) / 2 in
            try Hashtbl.iter (fun _ info ->
                    if info.match_index >= log_idx then (
                        incr nb_res ;
                        if has_quota () then (
                            L.debug "quota for submission (%d)!" !nb_res ;
                            raise Exit
                        ) else
                            L.debug "no quota for submission yet (%d)" !nb_res
                    )) peers_info ;
                if has_quota () then (
                    L.debug "quota for submission (%d)!" !nb_res ; true
                ) else (
                    L.debug "no quota for submission yet (%d)" !nb_res ; false
                )
            with Exit -> true

        (* where leaders try to increase their commit_index *)
        let try_commit t peers_info =
            for n = t.info.commit_index+1 to log_last_index t.logs do
                let entry = log_at t.logs n in
                if entry.term = t.info.current_term &&
                   is_committed t peers_info n then (
                    L.debug "%a: Entry %d is committed" print t n ;
                    t.info.commit_index <- n
                )
            done

        let try_apply t apply =
            while t.info.last_applied < t.info.commit_index do
                t.info.last_applied <- t.info.last_applied+1 ;
                let entry = log_at t.logs t.info.last_applied in
                L.debug "%a: Applying (%d,%a,%d)" print t t.info.last_applied Command.print entry.command entry.term ;
                let res = apply entry.command in
                entry.write (State (t.info.last_applied, res))
            done

        (* Used by followers *)
        let answer_append_entries t arg =
            assert (t.info.state = Follower) ;
            reset_election_timeout t ;
            let open RPC_Server_Types.AppendEntries in
            (* Reply false if term < currentTerm *)
            if arg.term < t.info.current_term then answer ~dest:arg.leader_id ~reason:"Too old to be a leader" t false else
            (* Reply false if log doesn't contain an entry at prev_log_index *)
            if arg.prev_log_index > log_last_index t.logs then
                let reason = Printf.sprintf "I have no entry at pos %d (my last log index is %d)"
                    arg.prev_log_index (log_last_index t.logs) in
                answer ~dest:arg.leader_id ~reason t false else
            (* or if this entry term does not match prev_log_term *)
            if arg.prev_log_index > 0 &&
               arg.prev_log_term <> (Option.get (log_at_opt t.logs arg.prev_log_index)).term then
                let reason = Printf.sprintf "my previous entry at pos %d was not term %d"
                                arg.prev_log_index arg.prev_log_term in
                answer ~dest:arg.leader_id ~reason t false
            else (
                let prev_len = log_last_index t.logs in
                (* all is good, try to append all new entries, truncating my logs if we notice an entry is wrong *)
                for i = 1 to Array.length arg.entries do
                    let idx = arg.prev_log_index + i in
                    match log_at_opt t.logs idx with
                    | Some entry ->
                        if arg.entries.(i-1).append_term <> entry.term then (
                            (* delete the existing entry and all that follow it *)
                            log_truncate t (idx-1) ;
                            log_append t.logs arg.entries.(i-1).append_command arg.entries.(i-1).append_term ignore
                        )
                    | None ->
                        log_append t.logs arg.entries.(i-1).append_command arg.entries.(i-1).append_term ignore
                done ;
                t.info.last_leader <- arg.RPC_Server_Types.AppendEntries.leader_id ;
                let new_commit_index =
                    if arg.leader_commit > t.info.commit_index then
                        let index_of_last_new_entry = arg.prev_log_index + Array.length arg.entries in
                        min arg.leader_commit index_of_last_new_entry
                    else t.info.commit_index in
                t.info.commit_index <- new_commit_index ;
                L.debug "%a: Growing logs from %d to %d entries, commit_index=%d" print t prev_len (log_last_index t.logs) new_commit_index ;
                answer ~dest:arg.leader_id t true
            )

        (* Send the entries to every other peers *)
        let append_entries ?(force=false) t peers_info =
            let now = Unix.gettimeofday () in
            List.iter (fun peer ->
                Hashtbl.modify_opt peer (function
                    | None -> (* create this info *)
                        Some { next_index = log_last_index t.logs + 1 ;
                               match_index = 0 ;
                               last_sent = now ;
                               in_flight = false }
                    | x -> x)
                    peers_info ;
                let info = Hashtbl.find peers_info peer in
                if not info.in_flight && 
                   (force || now -. info.last_sent > heartbeat_timeout) then (
                    info.in_flight <- true ;
                    info.last_sent <- now ;
                    let entries = append_log_from t.logs info.next_index in
                    let arg =
                        let open RPC_Server_Types.AppendEntries in
                        let prev_log_index = info.next_index-1 in
                        let prev_log_term =
                            if prev_log_index > 0 then (
                                let prev_log = log_at t.logs prev_log_index in
                                prev_log.term
                            ) else 0 in
                        { term = t.info.current_term ;
                          leader_id = t.info.host ;
                          prev_log_index ;
                          prev_log_term ;
                          entries ;
                          leader_commit = t.info.commit_index } in
                    call_server t peer (RPC_Server_Types.AppendEntries arg) (function
                        | Ok res ->
                            assert info.in_flight ;
                            info.in_flight <- false ;
                            if res.success then (
                                (* This peer received all our log *)
                                info.next_index <- info.next_index + Array.length arg.entries ; (* incr from what he received *)
                                info.match_index <- info.next_index - 1 ;
                                L.debug "%a: Received ack from %s, next_index will be %d" print t (Host.to_string peer) info.next_index
                            ) else (
                                (* The peer could find where to attach that *)
                                info.next_index <- info.next_index - 1 ;
                                L.debug "%a: Received nack from %s, next_index will be %d" print t (Host.to_string peer) info.next_index
                                (* TODO: retry faster *)
                            )
                        | Timeout | Err _ ->
                            L.error "%a: Cannot append entries to %s" print t (Host.to_string peer) ;
                            (* TODO: close? *))
                )
            ) t.info.peers

        let convert_to_leader t =
            L.debug "%a: Converting to Leader" print t ;
            assert (t.info.state = Candidate) ;
            let peers_info = Hashtbl.create (List.length t.info.peers) in
            t.info.state <- Leader peers_info ;
            append_entries ~force:true t peers_info (* will fill peers_info *)

        let convert_to_candidate t =
            L.debug "%a: Converting to Candidate" print t ;
            assert (t.info.state = Follower || t.info.state = Candidate) ;
            t.info.current_term <- t.info.current_term + 1 ;
            t.info.voted_for <- Some t.info.host ; (* I vote for myself (see below) *)
            t.info.state <- Candidate ;
            t.info.election_timeout <- random_election_timeout () ;
            reset_election_timeout t ;
            (* Request a vote *)
            if t.info.peers = [] then convert_to_leader t else
            let nb_wins = ref 1 (* I vote for myself (see above) *) in
            let last_log_index = log_last_index t.logs in
            let last_log_term =
                if last_log_index > 0 then
                    let last_log = log_at t.logs last_log_index in
                    last_log.term
                else 0 in
            let term = t.info.current_term in (* we save current_term so that we know when to discard answers *)
            iter_peers t (fun peer ->
                let open RPC_Server_Types.RequestVote in
                let arg = { term ; candidate_id = t.info.host ;
                            last_log_index ; last_log_term } in
                call_server t peer (RequestVote arg) (function
                    | Ok res ->
                        if res.success &&
                           res.term = term
                        then (
                            incr nb_wins ;
                            L.debug "%a: got %d wins" print t !nb_wins ;
                        ) else L.debug "%a: still only %d wins" print t !nb_wins ;
                        if t.info.state = Candidate && (* I'm still candidate *)
                           t.info.current_term = term && (* for *this* election *)
                           !nb_wins > (nb_peers t + 1) / 2
                        then convert_to_leader t
                    | Timeout | Err _ -> ()))

        let may_become_follower t peer_term =
            if peer_term > t.info.current_term then (
                L.debug "%a: Resetting current_term with peer's of %d" print t peer_term ;
                t.info.current_term <- peer_term ;
                t.info.voted_for <- None ;
                convert_to_follower t
            )

        let is_leader t = match t.info.state with Leader _ -> true | _ -> false

        let pub_of_raft (h : Host.t) = { h with port = h.port - 1 }
        let raft_of_pub (h : Host.t) = { h with port = h.port + 1 }

        let redirect t write =
            write (RPC_ClientServer_Types.Redirect (pub_of_raft t.info.last_leader))

        let serve pub_host peers apply =
            (* the raft host listen on next port compared to public host *)
            let raft_host = raft_of_pub pub_host in
            let raft_peers = List.map raft_of_pub peers in
            let t = init_follower raft_host raft_peers in
            (* Add a timeout function *)
            Event.register_timeout (fun _handler ->
                let now = Unix.gettimeofday () in
                let time_isolated = now -. t.info.last_recvd_from_leader in
                (match t.info.state with
                | Follower ->
                    if time_isolated > t.info.election_timeout then (
                        L.debug "%a: Timeouting (isolated for %gs)" print t time_isolated ;
                        convert_to_candidate t
                    )
                | Candidate ->
                    if time_isolated > t.info.election_timeout then (
                        L.debug "%a: Timeouting (isolated for %gs)" print t time_isolated ;
                        convert_to_candidate t
                    )
                | Leader peers_info ->
                    try_commit t peers_info ;
                    (* heartbeat *)
                    L.debug "%a: Heartbeat" print t ;
                    append_entries t peers_info) ;
                try_apply t apply) ;
            RPC_ClientServers.serve pub_host (fun write cmd ->
                match cmd with
                | ChangeState command ->
                    (match t.info.state with
                    | Leader peers_info ->
                        (* Leaders: if command received from client: append entry to local log, *)
                        log_append t.logs command t.info.current_term write ;
                        let new_entry_idx = log_last_index t.logs in
                        L.debug "%a: Received new command %a for index %d (commit_index=%d)" print t Command.print command new_entry_idx t.info.commit_index ;
                        (* issues AppendEntries in parallel to each of the other servers to
                         * replicate the entry. *)
                        append_entries ~force:nodelay_replication t peers_info
                    | _ ->
                        L.debug "%a: Received a command while I'm not the leader, redirecting to %s" print t (Host.to_string t.info.last_leader) ;
                        redirect t write)
                | QueryInfo ->
                    L.debug "%a: Must send infos" print t ;
                    write (Info t.info)) ;
            RPC_Servers.serve raft_host (fun write cmd ->
                match cmd with
                | RequestVote arg ->
                    let open RPC_Server_Types.RequestVote in
                    (* Ignore commands with old term *)
                    if arg.term >= t.info.current_term then (
                        may_become_follower t arg.term ;
                        write (answer_request_vote t arg)
                    )
                | AppendEntries arg ->
                    let open RPC_Server_Types.AppendEntries in
                    (* Ignore commands with old term *)
                    if arg.term >= t.info.current_term then (
                        may_become_follower t arg.term ;
                        if t.info.state = Candidate && arg.term = t.info.current_term then
                            convert_to_follower t ;
                        write (answer_append_entries t arg)
                    ))
    end

    (* We also have RPCs from clients to raft servers to append Commands.
     * Every command has a result (the result given by the apply function.
     * So answer to clients are delayed until actual application. Which
     * probably mean that the answer does not necessarily come from the same
     * server than the one we submitted the command to.
     * So the easier is: when we submit a command we get as the result the
     * log index of the command, and we also can pull the current state of
     * the state machine with its corresponding id, waiting for id > the one that
     * have previously been returned *)
    module Client =
    struct
        type t = { name : string ;
                   servers : Host.t array ; (* List of all known servers *)
                   mutable leader : Host.t (* the one believed to be good *) }

        let print fmt t =
            Printf.fprintf fmt "%20s@%s" (Log.colored true 2 "   Client") t.name

        let random_leader servers = servers.(Random.int (Array.length servers))

        let make name servers =
            { name ; servers ; leader = random_leader servers }

        let info ?(timeout=0.5) t host k =
            RPC_ClientServers.call ~timeout host QueryInfo (function
                | Ok (Info info) -> k info
                | Ok _ ->
                    L.debug "%a: server %s answering with state not infos" print t (Host.to_string host)
                | Timeout ->
                    L.error "%a: Timeouting getinfo to %s" print t (Host.to_string host)
                | Err x ->
                    L.error "%a: Can't get infos from %s: %s" print t (Host.to_string host) x)

        let call ?(timeout=0.5) t x k =
            let max_fast_try = Array.length t.servers * 2 in
            let max_slow_try = 4 in
            let rec retry nb_fast_try nb_slow_try =
                L.debug "%a: Sending %a to %s after %d fast, %d slow retries" print t Command.print x (Host.to_string t.leader) nb_fast_try nb_slow_try ;
                if nb_fast_try > max_fast_try then (
                    (* There might be an election going on, let's wait a little *)
                    if nb_slow_try >= max_slow_try then (
                        failwith "Too many retries"
                    ) else (
                        L.debug "%a: Pausing before retrying" print t ;
                        Event.pause 1. (fun () -> retry 0 (nb_slow_try+1))
                    )
                ) else (
                    let sent_to = t.leader in (* so that we can compare with redirection even after changing t.leader *)
                    RPC_ClientServers.call ~timeout t.leader (ChangeState x) (function
                        | Ok (State (log_index, state)) ->
                            L.debug "%a: Ack for (%d,%a)" print t log_index Command.print x ;
                            k state
                        | Ok (Redirect leader') ->
                            L.debug "%a: Was told by %s to redirect to %s" print t (Host.to_string sent_to) (Host.to_string leader') ;
                            t.leader <-
                                if leader' <> sent_to then leader' else
                                (* The server do not know who is the leader yet *)
                                if t.leader <> sent_to then t.leader else (
                                    (* And I still don't know neither: try one at random *)
                                    let l = until_diff t.leader (fun () -> random_leader t.servers) in
                                    L.debug "%a: Will redirect to %s instead" print t (Host.to_string l) ;
                                    l
                                ) ;
                            retry (nb_fast_try+1) nb_slow_try
                        | Ok (Info s) ->
                            L.debug "%a: server %s sending its status out of the blue?" print t (Host.to_string s.host) ;
                            assert false (* TODO *)
                        | Timeout ->
                            L.error "%a: Timeouting query %a to %s" print t Command.print x (Host.to_string sent_to) ;
                            retry (nb_fast_try+1) nb_slow_try
                        | Err x ->
                            L.error "%a: Can't get state from %s: %s" print t (Host.to_string t.leader) x ;
                            (* try another server? *)
                            t.leader <- random_leader t.servers ;
                            retry (nb_fast_try+1) nb_slow_try)
                )
            in
            retry 0 0
    end
end

(* FIXME:
 * Leader1 recoit command C, append_entry a tout le monde, mais pendant que cette entree se replique
 * un candidat prend la place de Leader1, qui ne peut donc submiter cette entree, et donc confirmer au
 * client. Au lieu de cela il redirige le client vers le nouveau Leader.
 * Leader2 possede l'entree C, mais elle n'a jamais ete submittee encore.
 * Le client resoumet la commande C a Leader2, qui l'accepte en nouvelle entree (on a donc alors deux
 * entrees pour la commande C a ce moment la.
 * Et les commits ne progresseront plus puisque la premiere commande C ne sera jamais repliquee completement.
 * Relire le papier!
 * *)
