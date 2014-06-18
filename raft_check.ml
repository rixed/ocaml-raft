(* How to stress test:
 * 1- log every apply operation (log index and actual command) in each
 *    server and at the end check that they are all the same.
 *    Also, check all the (index, op) in the client when they are acked,
 *    and check that they are all in the submitted log and there are
 *    nothing in the submitted log that was not acked.
 *    See check_output.ml for the checking program.
 *
 * 2- all communications must go through a proxy that will add delay/drops
 *    (fuzzing will be addressed separately by checksuming the PDUs).
 *    It should not be aware of the actual protocol but we are going to kill
 *    Marsal if we drop anything right now. So we must just delay for now,
 *    and protect against drop/fuzzing next.
 *)
open Batteries
open Raft_intf

module Command =
struct
    type t = int
    let print = Int.print
    type state = int
end

module Raft_impl = Raft.Make (Rpc.Tcp (Rpc.DefaultTcpConfig)) (Command)

module TestServer =
struct
    module StateMachine =
    struct
        let state = ref 0
        let apply v =
            state := ((!state + v) * v) mod 7919 ;
            !state
    end

    let serve host others =
        Raft_impl.Server.serve host others StateMachine.apply
end

module TestClient = Raft_impl.Client

let main =
    let nb_servers = 9 and nb_clients = 20 and nb_msgs = 100 in
    let servers = Array.init nb_servers (fun _ -> Host.make "localhost" (Random.int 64510 + 1024)) in
    Array.iter (fun s ->
        (* peers = list of all servers but s *)
        let peers = Array.fold_left (fun lst h -> if h = s then lst else h::lst) [] servers in
        TestServer.serve s peers)
        servers ;
    let clients = Array.init nb_clients (fun i -> TestClient.make ("Clt"^ string_of_int i)  servers) in
    (* Give the server a head start and make sure they elected a leader after 2*election_timeout *)
    let et = Raft.election_timeout in
    let nb_tests = ref 0 in
    Event.pause (2. *. et) (fun () ->
        (* Check there is only one leader *)
        (* We do this by sending a special 'dump' RPC to each servers (it works because we run this
         * in 'stopped clock' mode) *)
        let nb_leaders = ref 0 and nb_answers = ref 0 in
        Array.iter (fun s ->
            TestClient.info clients.(0) s (fun info ->
                incr nb_answers ;
                (match info.state with Leader _ -> incr nb_leaders | _ -> ())))
            servers ;
        (* wait until all responded *)
        Event.condition
            (fun () -> !nb_answers = Array.length servers)
            (fun () -> OUnit2.assert_equal !nb_leaders 1 ; incr nb_tests) ;
        (* Now start sending random commands. Each client must wait for confirmation of previous
         * command because otherwise we would not be certain of what order the commands were acked.
         * This does not change anything for the servers. *)
        let rec client_behavior nb_msg client =
            if nb_msg = 0 then incr nb_tests else
            TestClient.call client (Random.int 999) (fun _res ->
                client_behavior (nb_msg-1) client) in
        Array.iter (client_behavior nb_msgs) clients) ;
    Event.condition
        (fun () -> !nb_tests = 1 + Array.length clients)
        (fun () ->
            (* Wait a little bit more for raft servers to apply everything *)
            Event.pause (3. *. et) (fun () -> Event.clear ())) ;
    Event.loop ~timeout:(0.1 *. et) ()

