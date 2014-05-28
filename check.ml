(* Tests *)
open Batteries
open Raft_intf

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
        let open OUnit2 in
        RPC.call (1, 2) (fun r -> assert_equal r (Ok "3"))
end

let () =
    RPC_Checks.checks ()


