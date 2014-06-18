open Batteries
module L = Log.Info

let applications = Hashtbl.create 5
let acks = Hashtbl.create 5
let acked_log_idx = Hashtbl.create 51

let ack c (log_idx , _cmd as x) =
    let print = Tuple2.print Int.print Int.print in
    L.debug "Acking %a\n" print x ;
    Hashtbl.modify_def [] c (fun l -> x::l) acks ;
    (* A log_idx must be acked once *)
    Hashtbl.modify_opt log_idx (function
        | None -> Some c
        | Some c' as x ->
            Printf.eprintf "%s: Client %s then client %s both were acked log_idx %d\n!" (Log.colored true 1 "ERROR") c' c log_idx ;
            x)
        acked_log_idx

let check_acks () =
    (* Union of all clients acks must equ any of the app log.
     * Note that due to network latency clients may see the acks in a
     * different order than servers, so we reorder by log_idx. *)
    let acks =
        Hashtbl.keys acked_log_idx |>
        List.of_enum |>
        List.sort Int.compare in
    (* Compare the acks with any of the applications *)
    let apps =
        Hashtbl.values applications |>
        Enum.get_exn |>
        List.map Tuple3.first |>
        List.rev in
    let ok = apps = acks in
    Printf.printf "Comparing client acks with submitted values: %s\n"
        (if ok then "Ok" else "DIFFER") ;
    if not ok then
        Printf.printf "  Client acks: %a\n    Submitted: %a\n"
            (List.print Int.print) acks
            (List.print Int.print) apps ;
    ok

(* Check that all applied logs are the same on all servers *)

let apply h x =
    let print = Tuple3.print Int.print Int.print Int.print in
    L.debug "%s: Applying %a\n" h print x ;
    Hashtbl.modify_def [] h (fun l -> x::l) applications

let check_applications () =
    let compare_logs (h1, l1) (h2, l2) ok =
        let res = l1 = l2 in
        Printf.printf "Comparing %s (%d entries) and %s (%d): %s\n"
            h1 (List.length l1)
            h2 (List.length l2)
            (if res then "Ok" else "DIFFER") ;
        ok && res in
    let logs = Hashtbl.enum applications in
    Enum.fold2 compare_logs true logs (logs |> Enum.clone |> Enum.skip 1)


let () =
    let srv_re = Str.regexp ".*@\\([^,]+\\).*Applying (\\([0-9]+\\),\\([0-9]+\\),\\([0-9]+\\))"
    and clt_re = Str.regexp ".*@\\([^,]+\\).*Ack for (\\([0-9]+\\),\\([0-9]+\\))"
    in
    IO.lines_of stdin |>
    Enum.iter (fun str ->
        let g2i n = int_of_string (Str.matched_group n str) in
        let g2s n = Str.matched_group n str in
        if Str.string_match clt_re str 0 then (
            ack (g2s 1) (g2i 2, g2i 3)
        ) else if Str.string_match srv_re str 0 then (
            apply (g2s 1) (g2i 2, g2i 3, g2i 4)
        )
    ) ;
    let ok =
        check_applications () &&
        check_acks () in
    OUnit2.assert_equal ok true

