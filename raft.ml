open Batteries
open Raft_intf

type host_id = int
type term_id = int
type log_index = int (* first index is 1 *)

module type COMMAND = sig type t end

(* Implementation of a RAFT server *)
module Base (RPC_Maker : RPC.S) (Command : COMMAND) =
struct
    type log_entry =
        { command : Command.t (* command for the state machine *) ;
          term : term_id (* when it was received *) }
    type t =
        { mutable current_term : term_id (* latest term server has seen (initialized to 0 at startup, increases monotonically) *) ;
          mutable voted_for : host_id option (* candidate that received vote in current term (if any) *) ;
          mutable logs : log_entry list (* FIXME: a resizeable array for uncommitted and committed entries *) ;
          mutable commit_index : log_index (* index of highest log entry known to be committed *) ;
          mutable last_applied : log_index (* index of highest log entry known to be applied to state machine *) ;
          (* for the leader: *)
          next_index : (host_id, log_index * log_index) Hashtbl.t
            (* index of the next log entry and highest log entry known to be replicated on each server
             * (initialized to leader last log_index + 1) *) }

    let init () =
        { current_term = 0 ;
          voted_for = None ;
          logs = [] ;
          commit_index = 0 ;
          last_applied = 0 ;
          next_index = Hashtbl.create 9 }

    (* since we are going to try many logs implementation: *)
    (* raises Invalid_argument all kind of exception if idx is bogus *)
    let log_at t idx =
        List.at t.logs (idx - 1 (* first index is 1 *))

    module RPC_Types =
    struct
        (* There is only two RPCs *)
        module RequestVote =
        struct
            type arg =
                { term : term_id (* candidate's term *) ;
                  candidate_id : host_id (* candidate requesting vote *) ;
                  last_log_index : log_index (* index of candidate's last log entry *) ;
                  last_log_term : log_index (* term of candidate's last log entry *) }
        end
        module AppendEntries =
        struct
            type arg =
                { term : term_id (* leader's term *) ;
                  leader_id : host_id (* so follower can redirect clients *) ;
                  prev_log_index : log_index (* index of log entry immediately preceding new ones *) ;
                  prev_log_term : term_id (* term of prev_log_index entry *) ;
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
    module RPC = RPC_Maker (RPC_Types)
end

module Server (RPC_Maker : RPC.S) (Command : COMMAND) =
struct
    include Base (RPC_Maker) (Command)
    
    let answer t success =
        { RPC_Types.term = t.current_term ; RPC_Types.success = success }

    let request_vote t arg =
        let open RPC_Types.RequestVote in
        (* Reply false if term < current_term *)
        if arg.RPC_Types.RequestVote.term < t.current_term then answer t false else
        (* If voted_for is None or candidate_id, and candidate's log is at least as up-to-date
         * as receiver's log, grant vote *)
        let grant =
            (t.voted_for = None || t.voted_for = Some arg.candidate_id) &&
            arg.last_log_index >= List.length t.logs &&
            arg.last_log_term >= t.current_term in
        answer t grant

    let append_entries t apply arg =
        let open RPC_Types.AppendEntries in
        (* Reply false if term < currentTerm *)
        if arg.RPC_Types.AppendEntries.term < t.current_term then answer t false else
        (* Reply false if log doesn't contain an entry at prev_log_index whose term matches prev_log_term,
         * If an existing entry conflicts with a new one (same index but different terms),                 
         * delete the existing entry and all that follow it *)
        let rec aux new_logs_rev idx (to_append : log_entry list) logs =
            match to_append with
            | [] -> (* we are done *)
                true, List.rev new_logs_rev
            | _new_e::to_append' ->
                (match logs with
                | [] -> (* no more logs, append what we have *)
                    if idx > arg.prev_log_term then
                        true, List.rev_append new_logs_rev to_append
                    else false, [] (* arg was bogus *)
                | e::logs' ->
                    if idx < arg.prev_log_term then
                        aux (e::new_logs_rev) (idx+1) to_append logs'
                    else if idx = arg.prev_log_term then (
                        if e.term <> arg.prev_log_term then
                            false, []
                        else
                            aux (e::new_logs_rev) (idx+1) to_append logs'
                    ) else ( (* idx > arg.prev_log_term *)
                        if e.term <> arg.prev_log_term then
                            aux new_logs_rev idx to_append []
                        else (* we assume the e = new_e *)
                            aux (e::new_logs_rev) (idx+1) to_append' logs
                    )
                ) in
        let success, new_logs = aux [] 1 (Array.to_list arg.entries) t.logs in
        if not success then answer t false else (
            let new_commit_index =
                if arg.leader_commit > t.commit_index then
                    min arg.leader_commit (List.length new_logs)
                else t.commit_index in
            t.logs <- new_logs ;
            (* Apply all the new commands *)
            for idx = t.commit_index + 1 to new_commit_index do
                apply (log_at t idx)
            done ;
            t.commit_index <- new_commit_index ;
            (* TODO: what are we supposed to do with t.last_applied?? *)
            answer t true
        )

    let serve apply =
        let t = init () in
        RPC.serve (function
        | RequestVote arg -> request_vote t arg
        | AppendEntries arg -> append_entries t apply arg)

    (* We also have RPCs from clients to raft servers to append Command or read the state of the state machine;
     * which reading operation should probably be a command so that we read it at a given location in time,
     * after previous commands are applied - and that's the application of the read command that triggers
     * the response to the client. *)
end
