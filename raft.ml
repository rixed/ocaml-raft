open Batteries
open Raft_intf

module L = Log.Debug

type term_id = int
type log_index = int (* first index is 1 *)

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

    type log_entry =
        { command : Command.t (* command for the state machine *) ;
          term : term_id (* when it was received *) }

    type peer_info = { mutable next_index : log_index (* index of the next log entry to send there *) ;
                       mutable match_index : log_index (* index of highest log entry known to be replicated there *) ;
                       mutable last_sent : float (* when we last sent entries there *) }

    type leader_t = (Host.t, peer_info) Hashtbl.t
    type state = Follower | Candidate | Leader of leader_t

    let string_of_state = function
        | Follower -> Log.colored false 4 "Follower"
        | Candidate -> Log.colored true 4 "Candidate"
        | Leader _ -> Log.colored true 7 "Leader"

    type server =
        { mutable state : state (* The state I'm currently in *) ;
          host : Host.t (* myself *) ;
          peers : Host.t list (* other servers *) ;
          mutable current_term : term_id (* latest term server has seen (initialized to 0 at startup, increases monotonically) *) ;
          mutable voted_for : Host.t option (* candidate that received vote in current term (if any) *) ;
          mutable logs : log_entry list (* FIXME: a resizeable array like BatDynArray for uncommitted and committed entries *) ;
          mutable commit_index : log_index (* index of highest log entry known to be committed *) ;
          mutable last_leader : Host.t (* last leader we heard from *) ;
          mutable last_recvd_from_leader : float (* when we last heard from a leader *) ;
          mutable election_timeout : float }

    (* Same RPC_ClientServers for both Server and Client *)
    module RPC_ClientServer_Types =
    struct
        type arg = ChangeState of Command.t
                 | QueryInfo (* to a specific host *)
        type ret = State of log_index * Command.state (* to a ChangeState only *)
                 | Redirect of Host.t (* to a ChangeState only *)
                 | Info of server (* to a QueryInfo only *)
    end
    module RPC_ClientServers = RPC_Maker (RPC_ClientServer_Types)

    module Server =
    struct
        type t = server
        let print fmt t =
            Printf.fprintf fmt "%20s@%s, term=%d" (string_of_state t.state) (Host.to_string t.host) t.current_term

        module RPC_Server_Types =
        struct
            (* There is only two RPCs *)
            module RequestVote =
            struct
                type arg =
                    { term : term_id (* candidate's term *) ;
                      candidate_id : Host.t (* candidate requesting vote *) ;
                      last_log_index : log_index (* index of candidate's last log entry *) ;
                      last_log_term : term_id (* term of candidate's last log entry *) }
            end
            module AppendEntries =
            struct
                type arg =
                    { term : term_id (* leader's term *) ;
                      leader_id : Host.t (* so follower can redirect clients *) ;
                      prev_log_index : log_index (* index of log entry immediately preceding new ones (FIXME: what if the log was empty?) *) ;
                      prev_log_term : term_id (* term of prev_log_index entry (FIXME: idem) *) ;
                      entries : log_entry array (* log entries to store *) ;
                      leader_commit : log_index (* leader's commit_index *) }
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
                Printf.fprintf fmt "AppendEntries(nb_entries=%d)" (Array.length ae.entries)

        let init_follower host peers =
            { state = Follower ;
              election_timeout = random_election_timeout () ;
              current_term = 0 ;
              host ; peers ;
              voted_for = None ;
              logs = [] ;
              commit_index = 0 ;
              last_leader = host ; (* no better idea *)
              last_recvd_from_leader = Unix.gettimeofday () }

        let iter_peers t f =
            List.iter f t.peers

        let nb_peers t = List.length t.peers

        (* since we are going to try many logs implementation: *)
        (* raises Invalid_argument all kind of exception if idx is bogus *)
        let log_at logs idx =
            List.at logs (idx - 1 (* first index is 1 *))
        let log_at_option logs idx =
            try Some (List.at logs idx)
            with Invalid_argument _ -> None
        (* Return the list of logs starting at a given index *)
        let log_from logs idx =
            List.drop (idx-1) logs
        (* Append a new log entry and return last one's index *)
        let log_append t entry =
            t.logs <- t.logs @ [entry] ;
            List.length t.logs (* first index is 1 *)
        let log_last_index logs =
            List.length logs

        let reset_election_timeout t =
            let now = Unix.gettimeofday () in
            L.debug "%a: Resetting election timeout to %a" print t Log.date now ;
            t.last_recvd_from_leader <- now
            
        let convert_to_follower t =
            L.debug "%a: Converting to Follower" print t ;
            t.state <- Follower ;
            t.election_timeout <- random_election_timeout () ;
            reset_election_timeout t

        (* Wrapper that handles common behavior *)
        let call_server t peer arg k =
            L.debug "%a: Calling %s for %a" print t (Host.to_string peer) print_arg arg ;
            RPC_Servers.call peer arg (fun res ->
                (match res with
                | Ok res ->
                    if res.term > t.current_term then (
                        t.current_term <- res.term ;
                        t.voted_for <- None ;
                        convert_to_follower t
                    )
                | Err err ->
                    L.debug "%a: Received Error %s" print t err) ;
                k res)

        let answer ?dest ?reason t success =
            L.debug "%a: Answering %s%s%s"
                print t (if success then (Log.colored true 2 "YES") else (Log.colored false 1 "NO"))
                (match dest with Some d -> " to "^ Host.to_string d | None -> "")
                (match reason with Some r -> " because "^r | None -> "") ;
            { RPC_Server_Types.term = t.current_term ; RPC_Server_Types.success = success }

        let answer_request_vote t arg =
            let open RPC_Server_Types.RequestVote in
            let grant, reason =
                (* Reply false if term < current_term *)
                if arg.term < t.current_term then false, "he is from the past" else
                (* If voted_for is None or candidate_id, and candidate's log is at least as up-to-date
                 * as receiver's log, grant vote. *)
                match t.voted_for with Some guy when guy <> arg.candidate_id ->
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
            if grant then t.voted_for <- Some arg.candidate_id ;
            answer ~dest:arg.candidate_id ~reason t grant

        let log_and_apply t i apply =
            let l = log_at t.logs i in
            L.debug "%a: Applying (%d,%a,%d)" print t i Command.print l.command l.term ;
            apply l.command

        (* Used by followers *)
        let answer_append_entries t apply arg =
            assert (t.state = Follower) ;
            reset_election_timeout t ;
            let open RPC_Server_Types.AppendEntries in
            (* Reply false if term < currentTerm *)
            if arg.term < t.current_term then answer ~dest:arg.leader_id ~reason:"Too old to be a leader" t false else
            (* Reply false if log doesn't contain an entry at prev_log_index whose term matches prev_log_term *)
            if arg.prev_log_index > 0 &&
               (match log_at_option t.logs arg.prev_log_index with
                | Some log -> log.term <> arg.prev_log_term
                | _ -> true) then (
                answer ~dest:arg.leader_id ~reason:"I'm missing the previous log" t false
            ) else
            (* If an existing entry conflicts with a new one (same index but different terms),                 
             * delete the existing entry and all that follow it *)
            let rec aux new_logs_rev idx (to_append : log_entry list) logs =
                match to_append with
                | [] -> (* we are done *)
                    true, List.rev new_logs_rev
                | _new_e::to_append' ->
                    (match logs with
                    | [] -> (* no more logs, append what we have *)
                        if idx > arg.prev_log_index then
                            true, List.rev_append new_logs_rev to_append
                        else false, [] (* arg was bogus *)
                    | e::logs' ->
                        if idx < arg.prev_log_index then
                            aux (e::new_logs_rev) (idx+1) to_append logs'
                        else if idx = arg.prev_log_index then (
                            if e.term <> arg.prev_log_term then
                                false, []
                            else
                                aux (e::new_logs_rev) (idx+1) to_append logs'
                        ) else ( (* idx > arg.prev_log_index *)
                            if e.term <> arg.prev_log_term then
                                aux new_logs_rev idx to_append []
                            else (* we assume the e = new_e *)
                                aux (e::new_logs_rev) (idx+1) to_append' logs
                        )
                    ) in
            let success, new_logs = aux [] 1 (Array.to_list arg.entries) t.logs in
            if not success then answer ~dest:arg.leader_id t false
            else (
                t.last_leader <- arg.RPC_Server_Types.AppendEntries.leader_id ;
                let new_commit_index =
                    if arg.leader_commit > t.commit_index then
                        min arg.leader_commit (List.length new_logs)
                    else t.commit_index in
                t.logs <- new_logs ;
                (* We always apply what's committed *)
                for i = t.commit_index+1 to new_commit_index do
                    log_and_apply t i apply
                done ;
                t.commit_index <- new_commit_index ;
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
                               last_sent = now }
                    | x -> x)
                    peers_info ;
                let info = Hashtbl.find peers_info peer in
                let entries = log_from t.logs info.next_index in
                if force || now -. info.last_sent > heartbeat_timeout then (
                    info.last_sent <- now ;
                    let arg =
                        let open RPC_Server_Types.AppendEntries in
                        let prev_log_term =
                            if info.next_index > 1 then (
                                let prev_log = log_at t.logs (info.next_index-1) in
                                prev_log.term
                            ) else 0 in
                        { term = t.current_term ;
                          leader_id = t.host ;
                          prev_log_index = info.next_index-1 ;
                          prev_log_term ;
                          entries = Array.of_list entries ;
                          leader_commit = t.commit_index } in
                    call_server t peer (RPC_Server_Types.AppendEntries arg) (function
                        | Ok res ->
                            if res.success then (
                                (* This peer received all our log *)
                                info.next_index <- info.next_index + Array.length arg.entries ; (* incr from what he received *)
                                info.match_index <- info.next_index - 1
                            ) else (
                                (* The peer could find where to attach that *)
                                info.next_index <- info.next_index - 1
                                (* TODO: retry faster *)
                            )
                        | Err err ->
                            L.error "%a: Cannot append entries to %s: %s" print t (Host.to_string peer) err)
                )
            ) t.peers

        let convert_to_leader t =
            L.debug "%a: Converting to Leader" print t ;
            assert (t.state = Candidate) ;
            let peers_info = Hashtbl.create (List.length t.peers) in
            t.state <- Leader peers_info ;
            append_entries ~force:true t peers_info (* will fill peers_info *)

        let convert_to_candidate t =
            L.debug "%a: Converting to Candidate" print t ;
            assert (t.state = Follower || t.state = Candidate) ;
            t.current_term <- t.current_term + 1 ;
            t.voted_for <- Some t.host ; (* I vote for myself (see below) *)
            t.state <- Candidate ;
            t.election_timeout <- random_election_timeout () ;
            reset_election_timeout t ;
            (* Request a vote *)
            let nb_wins = ref 1 (* I vote for myself (see above) *) in
            let last_log_index = log_last_index t.logs in
            let last_log_term =
                if last_log_index > 0 then
                    let last_log = log_at t.logs last_log_index in
                    last_log.term
                else 0 in
            let term = t.current_term in (* we save current_term so that we know when to discard answers *)
            iter_peers t (fun peer ->
                let open RPC_Server_Types.RequestVote in
                let arg = { term ; candidate_id = t.host ;
                            last_log_index ; last_log_term } in
                call_server t peer (RequestVote arg) (function
                    | Ok res ->
                        if res.success &&
                           res.term = term
                        then (
                            incr nb_wins ;
                            L.debug "%a: got %d wins" print t !nb_wins ;
                        ) else L.debug "%a: still only %d wins" print t !nb_wins ;
                        if t.state = Candidate && (* I'm still candidate *)
                           t.current_term = term && (* for *this* election *)
                           !nb_wins > (nb_peers t + 1) / 2
                        then convert_to_leader t
                    | Err _err -> ()))

        let may_become_follower t peer_term =
            if peer_term > t.current_term then (
                L.debug "%a: Resetting current_term with peer's of %d" print t peer_term ;
                t.current_term <- peer_term ;
                t.voted_for <- None ;
                convert_to_follower t
            )

        let is_committed t peers_info log_idx =
            let nb_res = ref 1 in (* at least me *)
            try Hashtbl.iter (fun _ info ->
                    if info.match_index >= log_idx then (
                        incr nb_res ;
                        if !nb_res > (nb_peers t + 1) / 2 then raise Exit
                    )) peers_info ;
                false
            with Exit -> true

        let is_leader t = match t.state with Leader _ -> true | _ -> false

        let pub_of_raft (h : Host.t) = { h with port = h.port - 1 }
        let raft_of_pub (h : Host.t) = { h with port = h.port + 1 }

        let redirect t write =
            write (RPC_ClientServer_Types.Redirect (pub_of_raft t.last_leader))

        let serve pub_host peers apply =
            (* the raft host listen on next port compared to public host *)
            let raft_host = raft_of_pub pub_host in
            let raft_peers = List.map raft_of_pub peers in
            let t = init_follower raft_host raft_peers in
            (* Add a timeout function *)
            Event.register_timeout (fun _handler ->
                let now = Unix.gettimeofday () in
                let time_isolated = now -. t.last_recvd_from_leader in
                match t.state with
                | Follower ->
                    if time_isolated > t.election_timeout then (
                        L.debug "%a: Timeouting (isolated for %gs)" print t time_isolated ;
                        convert_to_candidate t
                    )
                | Candidate ->
                    if time_isolated > t.election_timeout then (
                        L.debug "%a: Timeouting (isolated for %gs)" print t time_isolated ;
                        convert_to_candidate t
                    )
                | Leader peers_info ->
                    (* heartbeat *)
                    append_entries t peers_info
            ) ;
            RPC_ClientServers.serve pub_host (fun write cmd ->
                match cmd with
                | ChangeState command ->
                    (match t.state with
                    | Leader peers_info ->
                        (* Leaders: if command received from client: append entry to local log, *)
                        let new_entry = { command ; term = t.current_term } in
                        let new_entry_idx = log_append t new_entry in
                        (* ...then issues AppendEntries in parallel to each of the other servers to
                         * replicate the entry. *)
                        append_entries ~force:nodelay_replication t peers_info ;
                        (* We have no idea when this will be committed, maybe only after several retries?
                         * So we enter a specific condition for every new entry, that's easy to check
                         * (if not fast) *)
                        Event.condition
                            (fun () ->
                                t.commit_index = new_entry_idx - 1 && (* since commit_index increase 1 by 1 on server, by construction below *)
                                is_committed t peers_info new_entry_idx)
                            (fun () ->
                                L.debug "%a: Entry %d is committed" print t new_entry_idx ;
                                (* When the entry has been committed the leader
                                 * applies the entry to its state machine and returns the result of that
                                 * execution to the client. *)
                                (* only do this if I'm still Leader *)
                                if is_leader t then (
                                    t.commit_index <- new_entry_idx ;
                                    let res = log_and_apply t new_entry_idx apply in
                                    write (State (new_entry_idx, res))
                                ) else (
                                    L.debug "%a: Too bad I'm not leader anymore :(" print t ;
                                    redirect t write
                                ))
                    | _ ->
                        L.debug "%a: Received a command while I'm not the leader, redirecting to %s" print t (Host.to_string t.last_leader) ;
                        redirect t write)
                | QueryInfo -> write (Info t)) ;
            RPC_Servers.serve raft_host (fun write cmd ->
                match cmd with
                | RequestVote arg ->
                    let open RPC_Server_Types.RequestVote in
                    (* Ignore commands with old term *)
                    if arg.term >= t.current_term then (
                        may_become_follower t arg.term ;
                        write (answer_request_vote t arg)
                    )
                | AppendEntries arg ->
                    let open RPC_Server_Types.AppendEntries in
                    (* Ignore commands with old term *)
                    if arg.term >= t.current_term then (
                        may_become_follower t arg.term ;
                        if t.state = Candidate && arg.term = t.current_term then
                            convert_to_follower t ;
                        write (answer_append_entries t apply arg)
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
        type t = { servers : Host.t array ; (* List of all known servers *)
                   mutable leader : Host.t (* the one believed to be good *) }

        let random_leader servers = servers.(Random.int (Array.length servers))

        let make servers =
            { servers ; leader = random_leader servers }

        let max_nb_try = 5

        let info _t host k =
            RPC_ClientServers.call host QueryInfo (function
                | Ok (Info info) -> k info
                | Ok _ ->
                    L.debug "Clt: server %s answering with state not infos" (Host.to_string host)
                | Err x ->
                    L.debug "Clt: Can't get infos from %s: %s" (Host.to_string host) x)

        let call t x k =
            let rec retry nb_try =
                L.debug "Clt: Sending command to %s after %d tries" (Host.to_string t.leader) nb_try ;
                if nb_try > max_nb_try then failwith "Too many retries" ;
                let sent_to = t.leader in (* so that we can compare with redirection even after changing t.leader *)
                RPC_ClientServers.call t.leader (ChangeState x) (function
                    | Ok (State (log_index, state)) ->
                        L.debug "Clt: Ack for (%d,%a)" log_index Command.print x ;
                        k state
                    | Ok (Redirect leader') ->
                        L.debug "Clt: Was told by %s to redirect to %s" (Host.to_string sent_to) (Host.to_string leader') ;
                        t.leader <-
                            if leader' <> sent_to then leader' else
                            (* The server do not know who is the leader yet *)
                            if t.leader <> sent_to then t.leader else (
                                (* And I still don't know neither: try one at random *)
                                let l = until_diff t.leader (fun () -> random_leader t.servers) in
                                L.debug "Clt: Will redirect to %s instead" (Host.to_string l) ;
                                l
                            ) ;
                        retry (nb_try+1)
                    | Ok (Info t) ->
                        L.debug "Clt: server %s sending its status out of the blue?" (Host.to_string t.host) ;
                        assert false (* TODO *)
                    | Err x ->
                        L.debug "Clt: Can't get state from %s: %s" (Host.to_string t.leader) x ;
                        (* try another server? *)
                        t.leader <- random_leader t.servers ;
                        retry (nb_try+1))
            in
            retry 0
    end
end
