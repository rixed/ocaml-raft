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

module Raft_impl = Raft.Make (Rpc.Local) (Command)

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

module TestClient =
struct
    let call h x k = Raft_impl.Client.call h x k
end

let main =
    let servers = Array.init 5 (fun _ -> Host.make "localhost" (string_of_int (Random.int 32768 + 1024))) in
    Array.iter (fun s ->
        (* peers = list of all servers but s *)
        let peers = Array.fold_left (fun lst h -> if h = s then lst else h::lst) [] servers in
        TestServer.serve s peers)
        servers ;
    TestClient.call servers 1 (fun r -> OUnit2.assert_equal r 1)

