(* Tests *)
open Batteries
open Raft_intf

(* Check TcpClient *)

module Tcp_Checks =
struct
    (* We simulate a 'string-length' service: client sends string and read their length *)
    module Clt = Event.TcpClient (struct type t_write = string type t_read = int end)
    module Srv = Event.TcpServer (struct type t_read = string type t_write = int end)
    let checks () =
        let oks = ref 0 and expected_res = ref 0 and idx = ref 0 in
        let tests = [| "hello" ; "glop" ; "" |] in
        let service_port = "31142" in
        Srv.serve service_port (fun w s -> String.length s |> w) ;
        let send = Clt.client "localhost" service_port (fun _w res ->
            if res = String.length tests.(!idx) then incr oks ;
            incr idx) in
        Array.iter send tests ;
        Event.loop () ;
        OUnit2.assert_equal !oks (Array.length tests)
end

(* Check RPCs *)

module RPC_Checks =
struct
    module RPC_Types =
    struct
        type arg = int * int
        type ret = string
    end
    module RPC = Rpc.Local(RPC_Types)

    let () =
        let f (a, b) = String.of_int (a+b) in
        RPC.serve f

    let checks () =
        RPC.call (1, 2) (fun r -> OUnit2.assert_equal r (Ok "3"))
end

let () =
    RPC_Checks.checks () ;
    Tcp_Checks.checks ()

