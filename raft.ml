open Batteries
open Raft_intf

type term_id = int
type log_index = int (* first index is 1 *)

module type COMMAND = sig
    type t (* the type of a command for the state machine *)
    type state (* the type of the value of the state machine, aka apply return value *)
end

(* We build both client and server from the same RPC_Maker and COMMAND so that type
 * correctness is enforced *and* we can actually use Rpc.Local *)
module Make (RPC_Maker : RPC.Maker) (Command : COMMAND) =
struct

    (* Same RPC_ClientServers for both Server and Client *)
    module RPC_ClientServer_Types =
    struct
        type arg = Command.t
        type ret = State of log_index * Command.state
                 | Redirect of Host.t
    end
    module RPC_ClientServers = RPC_Maker (RPC_ClientServer_Types)

    module Server =
    struct
        type log_entry =
            { command : Command.t (* command for the state machine *) ;
              term : term_id (* when it was received *) }

        type leader_t = { next_index : (Host.t, log_index * log_index) Hashtbl.t
                (* index of the next log entry and highest log entry known to be replicated on each server
                 * (initialized to leader last log_index + 1) *) }
        type state = Follower | Candidate | Leader of leader_t
        type t =
            { mutable state : state (* The state I'm currently in *) ;
              host : Host.t (* myself *) ;
              peers : Host.t list (* other servers *) ;
              mutable current_term : term_id (* latest term server has seen (initialized to 0 at startup, increases monotonically) *) ;
              mutable voted_for : Host.t option (* candidate that received vote in current term (if any) *) ;
              mutable logs : log_entry list (* FIXME: a resizeable array for uncommitted and committed entries *) ;
              mutable commit_index : log_index (* index of highest log entry known to be committed *) ;
              mutable last_applied : log_index (* index of highest log entry known to be applied to state machine *) ;
              mutable last_state : Command.state option (* last apply result *) ;
              mutable last_leader : Host.t (* last leader we heard from *) ;
              mutable last_recvd_from_leader : float (* when we last heard from a leader *) }

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

        let init_follower host peers =
            { state = Follower ;
              current_term = 0 ;
              host ; peers ;
              voted_for = None ;
              logs = [] ;
              commit_index = 0 ;
              last_applied = 0 ;
              last_state = None ;
              last_leader = host ; (* no better idea *)
              last_recvd_from_leader = Unix.time () }

        let iter_peers t f =
            List.iter f t.peers

        let nb_peers t = List.length t.peers

        (* since we are going to try many logs implementation: *)
        (* raises Invalid_argument all kind of exception if idx is bogus *)
        let log_at logs idx =
            List.at logs (idx - 1 (* first index is 1 *))
        (* Append a new log entry and return it's index *)
        let log_append t entries =
            t.logs <- t.logs @ entries ;
            List.length t.logs (* first index is 1 *)
        let log_last_index logs =
            List.length logs

        let reset_election_timeout t =
            t.last_recvd_from_leader <- Unix.time ()
            
        let convert_to_follower t =
            Log.debug "Srv@%s: Converting to Follower" (Host.to_string t.host) ;
            reset_election_timeout t ;
            failwith "TODO"

        (* Wrapper that handles common behavior *)
        let call_server t peer arg k =
            RPC_Servers.call peer arg (fun res ->
                (match res with
                | Ok res ->
                    if res.RPC_Server_Types.term > t.current_term then (
                        t.current_term <- res.term ;
                        convert_to_follower t
                    )
                | Err err ->
                    Log.debug "Srv@%s: Received Error %s" (Host.to_string t.host) err) ;
                k res)

        let answer t success =
            { RPC_Server_Types.term = t.current_term ; RPC_Server_Types.success = success }

        let answer_request_vote t arg =
            let open RPC_Server_Types.RequestVote in
            (* Reply false if term < current_term *)
            if arg.RPC_Server_Types.RequestVote.term < t.current_term then answer t false else
            (* If voted_for is None or candidate_id, and candidate's log is at least as up-to-date
             * as receiver's log, grant vote *)
            let grant =
                (t.voted_for = None || t.voted_for = Some arg.candidate_id) &&
                arg.last_log_index >= List.length t.logs &&
                arg.last_log_term >= t.current_term in
            answer t grant

        (* Used by followers *)
        let answer_append_entries t apply arg =
            assert (t.state = Follower) ;
            let open RPC_Server_Types.AppendEntries in
            (* Reply false if term < currentTerm *)
            if arg.term < t.current_term then answer t false else
            (* Reply false if log doesn't contain an entry at prev_log_index whose term matches prev_log_term *)
            if arg.prev_log_index > 0 && (let log = log_at t.logs arg.prev_log_index in log.term) <> arg.prev_log_term then (
                answer t false
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
            if not success then answer t false
            else (
                t.last_leader <- arg.RPC_Server_Types.AppendEntries.leader_id ;
                let new_commit_index =
                    if arg.leader_commit > t.commit_index then
                        min arg.leader_commit (List.length new_logs)
                    else t.commit_index in
                t.logs <- new_logs ;
                (* Apply all the new commands *)
                t.commit_index <- new_commit_index ;
                while t.last_applied < t.commit_index do
                    t.last_applied <- t.last_applied + 1 ;
                    t.last_state <- Some (apply (log_at t.logs t.last_applied).command)
                done ;
                answer t true
            )

        (* Send the entries to every other peers *)
        let append_entries t entries k =
            let new_idx = log_append t entries in
            assert (new_idx > 0) ;
            let arg =
                let open RPC_Server_Types.AppendEntries in
                let prev_log_term =
                    if new_idx > 1 then (
                        let prev_log = log_at t.logs (new_idx-1) in
                        prev_log.term
                    ) else -1 in
                { term = t.current_term ;
                  leader_id = t.host ;
                  prev_log_index = new_idx-1 ;
                  prev_log_term ;
                  entries = Array.of_list entries ;
                  leader_commit = t.commit_index } in
            iter_peers t (fun peer ->
                Log.debug "Srv@%s: Send an AppendEntries (with %d entry) to follower %s:%s" (Host.to_string t.host) (List.length entries) peer.name peer.port ;
                call_server t peer (RPC_Server_Types.AppendEntries arg) k)

        let convert_to_leader t =
            Log.debug "Srv@%s: Converting to Leader" (Host.to_string t.host) ;
            assert (t.state = Candidate) ;
            let next_index = Hashtbl.create (List.length t.peers) in
            List.iter (fun p ->
                Hashtbl.add next_index p (0, 0))
                t.peers ; (* FIXME: how to init this, really? *)
            t.state <- Leader { next_index } ;
            append_entries t [] ignore (* just to say hello *)

        let convert_to_candidate t =
            Log.debug "Srv@%s: Converting to Candidate" (Host.to_string t.host) ;
            assert (t.state = Follower || t.state = Candidate) ;
            t.current_term <- t.current_term + 1 ;
            t.state <- Candidate ;
            reset_election_timeout t ;
            (* Request a vote *)
            let nb_wins = ref 1 (* I vote for myself *) in
            let last_log_index = log_last_index t.logs in
            let last_log_term =
                let last_log = log_at t.logs last_log_index in
                last_log.term in
            let term = t.current_term in (* we save current_term so that we know when to discard answers *)
            iter_peers t (fun peer ->
                let open RPC_Server_Types.RequestVote in
                let arg = { term ; candidate_id = t.host ;
                            last_log_index ; last_log_term } in
                call_server t peer (RequestVote arg) (function
                    | Ok res ->
                        if res.success &&
                           res.term = term &&
                           t.current_term = term && (* I'm still candidate for *this* election *)
                           t.state = Candidate (* Which guarantee we won't enter here again once win *)
                        then (
                            incr nb_wins ;
                            if !nb_wins > nb_peers t / 2 then convert_to_leader t
                        )
                    | Err _err -> ()))

        let may_become_follower t peer_term =
            if peer_term >= t.current_term then (
                if peer_term > t.current_term then (
                    Log.debug "Srv@%s: Resetting current_term with peer's of %d" (Host.to_string t.host) peer_term ;
                    t.current_term <- peer_term
                ) ;
                convert_to_follower t
            )

        let may_send_heartbeat lt now =
            ignore lt ;
            ignore now ;
            failwith "TODO"

        let serve ?(election_timeout=3.) host peers apply =
            let t = init_follower host peers in
            (* Add a timeout function *)
            Event.register_timeout (fun _handler ->
                let now = Unix.time () in
                match t.state with
                | Follower ->
                    if now -. t.last_recvd_from_leader > election_timeout then
                        convert_to_candidate t
                | Candidate ->
                    if now -. t.last_recvd_from_leader > election_timeout then
                        (* TODO: contermeasure for repeated split votes, Cf 5.2 *)
                        convert_to_candidate t
                | Leader lt ->
                    may_send_heartbeat lt now
            ) ;
            RPC_ClientServers.serve host (fun command ->
                (match t.state with
                | Leader _ ->
                    (* Leaders: if command received from client: append entry to local log, *)
                    let new_entry = { command ; term = t.current_term } in
                    (* ...then issues AppendEntries in parallel to each of the other servers to
                     * replicate the entry. *)
                    (* TODO: send only this entry? *)
                    let nb_res = ref 0 in
                    append_entries t [new_entry] (fun _res ->
                        incr nb_res ;
                        Log.debug "Srv@%s: Received %d answers from followers" (Host.to_string t.host) !nb_res) ;
                    (* TODO: This is my understanding that we cannot serve anything else until this query is answered *)
                    while !nb_res < nb_peers t do
                        Unix.sleep 1 (* FIXME: condvar *)
                    done ;
                    (* When the entry has been safely replicated the leader
                     * applies the entry to its state machine and returns the result of that
                     * execution to the client. *)
                    let res = apply command in
                    State (List.length t.logs, res)
                | _ ->
                    Log.debug "Srv@%s: Received a command while I'm not the leader, redirecting to %s" (Host.to_string t.host) (Host.to_string t.last_leader) ;
                    Redirect t.last_leader)) ;
            RPC_Servers.serve host (function
                | RequestVote arg ->
                    may_become_follower t arg.RPC_Server_Types.RequestVote.term ;
                    answer_request_vote t arg
                | AppendEntries arg ->
                    may_become_follower t arg.RPC_Server_Types.AppendEntries.term ;
                    answer_append_entries t apply arg)
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
        let call ?(max_nb_try=5) servers x k =
            let rec retry nb_try leader =
                if nb_try > max_nb_try then failwith "Too many retries" ;
                RPC_ClientServers.call leader x (function
                    | Ok (State (_log_index, state)) ->
                        k state
                    | Ok (Redirect leader') ->
                        Log.debug "Clt: Was told to redirect to %s" (Host.to_string leader') ;
                        let leader' =
                            if leader' <> leader then leader' else (
                                (* The server do not know who is the leader yet, try another one *)
                                servers.(Random.int (Array.length servers))
                            ) in
                        retry (nb_try+1) leader'
                    | Err _ ->
                        (* try another server? *)
                        let leader' = servers.(Random.int (Array.length servers)) in
                        retry (nb_try+1) leader')
            in
            retry 0 servers.(0)
    end
end
