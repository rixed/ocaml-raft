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
    let servers = Array.init 5 (fun _ -> Host.make "localhost" (Random.int 64510 + 1024)) in
    Array.iter (fun s ->
        (* peers = list of all servers but s *)
        let peers = Array.fold_left (fun lst h -> if h = s then lst else h::lst) [] servers in
        TestServer.serve s peers)
        servers ;
    let client = TestClient.make servers in
    (* Give the server a head start and make sure they elected a leader after 2*election_timeout *)
    let et = Raft.election_timeout in
    let nb_tests = ref 0 in
    Event.pause (2. *. et) (fun () ->
        (* Check there is only one leader *)
        (* We do this by sending a special 'dump' RPC to each servers (it works because we run this
         * in 'stopped clock' mode) *)
        let nb_leaders = ref 0 and nb_answers = ref 0 in
        Array.iter (fun s ->
            TestClient.info client s (fun info ->
                incr nb_answers ;
                (match info.state with Leader _ -> incr nb_leaders | _ -> ())))
            servers ;
        (* wait until all responded *)
        Event.condition
            (fun () -> !nb_answers = Array.length servers)
            (fun () -> OUnit2.assert_equal !nb_leaders 1 ; incr nb_tests) ;
        Event.pause (3. *. et) (fun () ->
            let do_test i _o =
                Log.info "Sending %d..." i ;
                TestClient.call client i (fun r ->
                    Log.info "Sent %d, got %d" i r ;
                    (*OUnit2.assert_equal r o ;*)
                    incr nb_tests) in
            (* Apart from that, start perturbing the raft servers by sending some commands to the state machine *)
            do_test 1 1 ;
            do_test 2 6 ;
            do_test 3 27)) ;
    Event.condition
        (fun () -> !nb_tests = 4)
        (fun () ->
            (* Wait a little bit more for raft servers to apply everything *)
            Event.pause (3. *. et) (fun () -> Event.clear ())) ;
    Event.loop ~timeout:(0.1 *. et) ()

