open Batteries
open Raft_intf

module L = Log.Info

(* RPC through TCP *)

(* for clarity, have a single id for any kind of TCP *)
let next_id =
    let n = ref 0 in
    fun () -> incr n ; !n

module type TCP_CONFIG =
sig
    val cnx_timeout : float
    val max_accepted : int option
end

module DefaultTcpConfig : TCP_CONFIG =
struct
    let cnx_timeout = 1.
    let max_accepted = None
end

module Tcp (Config : TCP_CONFIG) (Types : RPC.TYPES) : RPC.S with module Types = Types =
struct
    module Types = Types

    type rpc_res = Ok of Types.ret
                 | Timeout
                 | Err of string

    type id = int

    module BaseIOType =
    struct
        type t_read = id * Types.arg
        type t_write = id * Types.ret
    end
    module Srv_IOType = MakeIOType(BaseIOType)
    module Srv_Pdu = Pdu.Marshaller(Srv_IOType)
    module TcpServer = Event.TcpServer(Srv_IOType)(Srv_Pdu)

    open Config

    let serve h f =
        let _shutdown = TcpServer.serve ~cnx_timeout ?max_accepted (string_of_int h.Server.port) (fun write input ->
            match input with
            | Srv_IOType.Value (id, v) ->
                f (fun res -> write (Srv_IOType.Write (id, res))) v
            | Srv_IOType.Timeout _
            | Srv_IOType.EndOfFile ->
                write Srv_IOType.Close) in
        () (* we keep serving until we die *)

    module Clt_IOType = MakeIOTypeRev(BaseIOType)
    module Clt_Pdu = Pdu.Marshaller(Clt_IOType)
    module TcpClient = Event.TcpClient(Clt_IOType)(Clt_Pdu)

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
     *   speack to the same dest
     *)
    let cnxs = Hashtbl.create 31
    let continuations = Hashtbl.create 72

    (* timeout continuations *)
    let try_timeout id =
        match  Hashtbl.find_option continuations id with
        | Some k ->
            L.debug "Timeouting message id %d" id ;
            Hashtbl.remove continuations id ;
            k Timeout
        | None -> ()

    let call ?(timeout=0.5) h v k =
        let writer =
            match Hashtbl.find_option cnxs h with
            | Some w -> w
            | None ->
                (* connect to the server *)
                L.debug "Need a new connection to %s" (Server.to_string h) ;
                let w = TcpClient.client ~cnx_timeout h.Server.name (string_of_int h.Server.port) (fun write input ->
                    match input with
                    | Clt_IOType.Value (id, v) ->
                        (* Note: we can't modify continuations in place because we'd
                         * have to call write before returning to modify_opt, and write
                         * can itself update the hash. *)
                        (match Hashtbl.find_option continuations id with
                        | None ->
                            L.error "No continuation for message id %d (already timeouted?)" id
                        | Some k ->
                            L.debug "Continuing message id %d" id ;
                            k (Ok v) ;
                            Hashtbl.remove continuations id)
                    | Clt_IOType.Timeout _now -> (* called when the underlying IO had nothing to read for too long *)
                        ()
                    | Clt_IOType.EndOfFile ->
                        (* since we don't know which messages were sent via this cnx, rely on timeout to notify continuations *)
                        L.info "Closing cnx to %s" (Server.to_string h) ;
                        write Close ;
                        Hashtbl.remove cnxs h) in
                Hashtbl.add cnxs h w ;
                w in
        let id = next_id () in
        L.debug "Saving continuation for message id %d" id ;
        Event.pause timeout (fun () -> try_timeout id) ;
        Hashtbl.add continuations id k ;
        writer (Write (id, v))

end
