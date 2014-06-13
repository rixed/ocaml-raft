(* Tests *)
open Batteries
open Raft_intf

(* Check TcpClient *)

module Tcp_Checks =
struct
    module BaseIOType =
    struct
        type t_write = string
        let print_t_write = String.print
        type t_read = int
        let print_t_read = Int.print
    end
    (* We simulate a 'string-length' service: client sends string and read their length *)
    module CltT = Event.MakeIOType (BaseIOType)
    module SrvT = Event.MakeIOTypeRev (BaseIOType)
    module Clt = Event.TcpClient (CltT)
    module Srv = Event.TcpServer (SrvT)
    let checks () =
        let oks = ref 0 and idx = ref 0 in
        let tests = [| "hello" ; "glop" ; "" |] in
        let service_port = "31142" in
        let stop_listening = ref ignore in
        let server w = function
            | SrvT.EndOfFile ->
                w SrvT.Close ;
                !stop_listening ()
            | SrvT.Value s ->
                Log.debug "Serving string.length for str='%s'" s ;
                SrvT.Write (String.length s) |> w in
        stop_listening := Srv.serve service_port server ;
        let send = Clt.client "localhost" service_port (fun w res ->
            match res with
            | CltT.EndOfFile ->
                w CltT.Close
            | CltT.Value l ->
                if l = String.length tests.(!idx) then incr oks ;
                incr idx) in
        Array.iter (fun s -> send (CltT.Write s)) tests ;
        send CltT.Close;
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

