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
