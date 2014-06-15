open Batteries
open Raft_intf

(* RPC through TCP *)

module type TCP_CONFIG =
sig
    val timeout : float option
    val max_accepted : int option
end

module DefaultTcpConfig : TCP_CONFIG =
struct
    let timeout = None
    let max_accepted = None
end

module Tcp (Config : TCP_CONFIG) (Types : RPC.TYPES) : RPC.S with module Types = Types =
struct
    module Types = Types

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

    let serve h f =
        let _shutdown = TcpServer.serve ?timeout ?max_accepted (string_of_int h.Host.port) (fun write input ->
            match input with
            | Srv_IOType.Value (id, v) ->
                f (fun res -> write (Srv_IOType.Write (id, res))) v
            | Srv_IOType.Timeout
            | Srv_IOType.EndOfFile ->
                write Srv_IOType.Close) in
        () (* we keep serving until we die *)

    module Clt_IOType = Event.MakeIOTypeRev(BaseIOType)
    module TcpClient = Event.TcpClient(Clt_IOType)

    (* Notice that:
     * - the TCP cnx is initialized when first used and then saved for later,
     *   so that it's easier for the client (no need to keep out state along) and
     *   also it doesn't have to explicitly connect to a new place and handle the connection
     *   termination itself. This cost a hashtable lookup, though.
     * - we need to associate an id with each query and store every continuations in a hash to send the proper
     *   answer to the proper continuation, since you may call the server several times before an answer is
     *   received, and the server is not constrained to answer in sequence (since it may itself depend on a
     *   backend). Since the hash of cnx is global and the id is global as well, we can imagine a query
     *   being answered by another server, which is cool or frightening.
     * - as a result, if we store several servers on this program they can share the same cnxs if they
     *   speack to the same dest, which is very cool!
     *)
    let cnxs = Hashtbl.create 31
    let continuations = Hashtbl.create 72
    let next_id =
        let n = ref 0 in
        fun () -> incr n ; !n

    let call h v k =
        let writer =
            match Hashtbl.find_option cnxs h with
            | Some w -> w
            | None ->
                (* connect to the server *)
                let w = TcpClient.client ?timeout h.Host.name (string_of_int h.Host.port) (fun write input ->
                    match input with
                    | Clt_IOType.Value (id, v) ->
                        Hashtbl.modify_opt id (function
                            | None -> failwith "TODO"
                            | Some k -> k (Ok v) ; None)
                            continuations ;
                    | Clt_IOType.Timeout
                    | Clt_IOType.EndOfFile ->
                        (* notify all continuations *)
                        Hashtbl.iter (fun _id k ->
                            k (Err "Connection closed")) continuations ;
                        write Close ;
                        Hashtbl.remove cnxs h) in
                Hashtbl.add cnxs h w ;
                w in
        let id = next_id () in
        Hashtbl.add continuations id k ;
        writer (Write (id, v))

end
