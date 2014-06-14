open Batteries
open Raft_intf

(* not remote RPC, ie call function directly *)
module Local : RPC.S = functor (Types : RPC.TYPES) ->
struct
    type rpc_res = Ok of Types.ret
                 | Err of string

    let server = ref None

    let call a k =
        match !server with
        | None -> k (Err "no server")
        | Some srv -> k (Ok (srv a))

    let serve f =
        server := Some f
end

(* RPC through TCP *)

module type TCP_CONFIG =
sig
    val timeout : float option
    val max_accepted : int option
    val service_port : string
    val server_name : string
end

module DefaultTcpConfig : TCP_CONFIG =
struct
    let timeout = None
    let max_accepted = None
    let service_port = "27481"
    let server_name = "localhost"
end

module Tcp (Config : TCP_CONFIG) : RPC.S = functor (Types : RPC.TYPES) ->
struct
    type rpc_res = Ok of Types.ret
                 | Err of string

    type id = int

    module BaseIOType =
    struct
        type t_read = id * Types.arg
        type t_write = id * Types.ret
    end
    module Srv_IOType = Event.MakeIOType(BaseIOType)
    module TcpServer = Event.TcpServer(Srv_IOType)

    open Config

    let serve f =
        let _shutdown = TcpServer.serve ?timeout ?max_accepted service_port (fun write input ->
            let res = match input with
                | Srv_IOType.Value (id, v) -> Srv_IOType.Write (id, f v)
                | Srv_IOType.EndOfFile -> Srv_IOType.Close in
            write res) in
        () (* we keep serving until we die *)

    module Clt_IOType = Event.MakeIOTypeRev(BaseIOType)
    module TcpClient = Event.TcpClient(Clt_IOType)

    (* Notice that:
     * - the TCP cnx is initialized at the first time. Since it is the same for all service_port*server_name,
     *   it makes sense to have one global cnx for this module.
     * - we need to associate an id with each query and store every continuations in a hash to send the proper
     *   answer to the proper continuation, since you may call the server several times before an answer is
     *   received, and the server is not constraint to answer in sequence (since it may itself depend on a
     *   backend).
     *)
    let cnx_writer = ref None
    let continuations = Hashtbl.create 72
    let next_id =
        let n = ref 0 in
        fun () -> incr n ; !n

    let call v k =
        if !cnx_writer = None then
            (* connect to the server *)
            cnx_writer := Some (TcpClient.client ?timeout server_name service_port (fun write input ->
                match input with
                | Clt_IOType.Value (id, v) ->
                    Hashtbl.modify_opt id (function
                        | None -> failwith "TODO"
                        | Some k -> k (Ok v) ; None)
                        continuations ;
                | Clt_IOType.EndOfFile ->
                    (* notify all continuations *)
                    Hashtbl.iter (fun _id k ->
                        k (Err "Connection closed")) continuations ;
                    write Close ;
                    cnx_writer := None)) ;
        let id = next_id () in
        Hashtbl.add continuations id k ;
        (Option.default ignore !cnx_writer) (Write (id, v))

end
