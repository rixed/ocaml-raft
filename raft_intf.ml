(* All signatures for the whole library *)

module RPC =
struct
    module type TYPES =
    sig
        type arg
        type ret
    end

    module type S = functor (Types:TYPES) ->
    sig
        (* Retriable errors will be retried *)
        type rpc_res = Ok of Types.ret (* TODO: use BatResult? *)
                     | Err of string

        (* We favor event driven programming here *)
        val call : Types.arg -> (rpc_res -> unit) -> unit
        (* TODO: a call_multiple that allow several answers to be received. Useful to 
         * implement pubsub *)
        val serve : (Types.arg -> Types.ret) -> unit
    end
end

