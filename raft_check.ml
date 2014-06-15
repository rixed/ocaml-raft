(* To stress test RAFT for real, we start M clients and pick at random a list
 * of pair (client_idx, request). Then we create a set a N servers
 * and start playing the sequence of queries while perturbing the servers.
 * At the end we save the last result.
 * We restart with N' servers, perturbing them in a different way, and check
 * we obtain the same result.
 * Etc...
 *
 * The first run should start with a single, reliable server, so that we know
 * the expected result!
 *
 * If the result differ then we want to be able to see a detailed log of
 * what happened, so log everything exhaustively!
 *
 * Note that the RPC mechanism we need here can be as simple as local function
 * call, since the random query list is the sequence in which the leader receive
 * the message (so any effect due to reordering or redirections from followers
 * to leader are already included).
 *
 * The service is simple yet depend on the order of execution: add to the current
 * value with the value provided by client, then multiply by it, and take the modulo.
 *)
open Batteries
open Raft_intf

module Command =
struct
    type t = int
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
            (fun () ->
                OUnit2.assert_equal !nb_leaders 1 ;
                Event.clear ()) ;
        (* Apart from that, start perturbing the raft servers by sending some commands to the state machine *)
        TestClient.call client 1 (fun r -> OUnit2.assert_equal r 1)) ;
    Event.loop ~timeout:(0.1 *. et) ()

