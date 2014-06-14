(* All signatures for the whole library *)

module Host =
struct
    type t = { name : string ;
               port : string }
    let make name port = { name ; port }
    let to_string t = t.name ^":"^ t.port
end

module RPC =
struct
    module type TYPES =
    sig
        type arg
        type ret
    end

    module type S =
    sig
        module Types : TYPES

        (* Retriable errors will be retried *)
        type rpc_res = Ok of Types.ret
                     | Err of string

        (* We favor event driven programming here *)
        val call : Host.t -> Types.arg -> (rpc_res -> unit) -> unit
        (* TODO: a call_multiple that allows several answers to be received. Useful to 
         * implement pubsub *)
        val serve : Host.t -> (Types.arg -> Types.ret) -> unit
    end

    module type Maker = functor(Types : TYPES) -> (S with module Types = Types)
end

