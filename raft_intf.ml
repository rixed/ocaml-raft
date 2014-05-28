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
        type rpc_res = Ok of Types.ret
                     | Err of string

        (* We favor event driven programming here *)
        val call : Types.arg -> (rpc_res -> unit) -> unit
        val serve : (Types.arg -> Types.ret) -> unit
    end
end

