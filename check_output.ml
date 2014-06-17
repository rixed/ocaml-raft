open Batteries

let applications = Hashtbl.create 5
let acks = ref []

let ack x =
    let print = Tuple2.print Int.print Int.print in
    Printf.printf "Acking %a\n" print x ;
    acks := x::!acks

let check_acks () =
    (* Compare the acks with any of the applications *)
    let apps =
        Hashtbl.values applications |>
        Enum.get_exn |>
        List.map Tuple3.get12 in
    let ok = apps = !acks in
    Printf.printf "Comparing client acks with submitted values: %s\n"
        (if ok then "Ok" else "DIFFER") ;
    ok

(* Check that all applied logs are the same on all servers *)

let apply h x =
    let print = Tuple3.print Int.print Int.print Int.print in
    Printf.printf "%s: Applying %a\n" h print x ;
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
    and clt_re = Str.regexp ".*Ack for (\\([0-9]+\\),\\([0-9]+\\))"
    in
    IO.lines_of stdin |>
    Enum.iter (fun str ->
        let g2i n = int_of_string (Str.matched_group n str) in
        let g2s n = Str.matched_group n str in
        if Str.string_match clt_re str 0 then (
            ack (g2i 1, g2i 2)
        ) else if Str.string_match srv_re str 0 then (
            apply (g2s 1) (g2i 2, g2i 3, g2i 4)
        )
    ) ;
    let ok =
        check_applications () &&
        check_acks () in
    OUnit2.assert_equal ok true

