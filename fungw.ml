open Batteries
open Raft_intf

module L = Log.Debug

(* This program merely:
 * listen to some port and connect to a given destination, then
 * forward what it receives to this destination, while introducing:
 * - some amount of random lag
 * - some amount of random loss
 * - some amount of random fuzzing
 *
 * These amounts are changing from time to time within given bounds,
 * for each individual links or set of links.
 *)

let random_duration () =
    Random.float 10.

(* ratio to percent *)
let r2p r = r *. 100.

module Setting =
struct
    type t =
        { mutable min : float ;
          mutable max : float }

    let print fmt t =
        Format.fprintf fmt "from %g%% to %g%%" (r2p t.min) (r2p t.max)

    let make min max =
        assert (min <= max) ;
        { min ; max }

    let make_ok () =
        { min = 0. ; max = 0. }

    let from_string str =
        let min, max = String.split str "-" in
        make (float_of_string min) (float_of_string max)
end

module Config =
struct
    type t =
        { mutable lag : Setting.t ;
          mutable loss : Setting.t ;
          mutable fuzzing : Setting.t }

    let print fmt t =
        Format.fprintf fmt "lag: %a, loss: %a, fuzzing: %a"
            Setting.print t.lag
            Setting.print t.loss
            Setting.print t.fuzzing

    let make_ok () =
        { lag = Setting.make_ok () ;
          loss = Setting.make_ok () ;
          fuzzing = Setting.make_ok () }

    let make lag loss fuzzing =
        { lag ; loss ; fuzzing }
end

module Degradation =
struct
    type t = { chance : float ; duration : float }

    let print fmt t =
        Format.fprintf fmt "%g%% chance for %gs" (r2p t.chance) t.duration

    let make setting =
        let open Setting in
        { chance = Random.float (setting.max -. setting.min) +. setting.min ;
          duration = random_duration () }

    let make_ok () =
        { chance = 0. ;
          duration = random_duration () }
end

module Condition =
struct
    type t =
        { lag : Degradation.t ;
          loss : Degradation.t ;
          fuzzing : Degradation.t }

    let print fmt t =
        Format.fprintf fmt "lag: %a, loss: %a, fuzzing: %a"
            Degradation.print t.lag
            Degradation.print t.loss
            Degradation.print t.fuzzing

    let make conf =
        { lag = Degradation.make conf.Config.lag ;
          loss = Degradation.make conf.Config.loss ;
          fuzzing = Degradation.make conf.Config.fuzzing }

    let make_ok () =
        { lag = Degradation.make_ok () ;
          loss = Degradation.make_ok () ;
          fuzzing = Degradation.make_ok () }

end

module Link =
struct
    module IOType = MakeIOType(Pdu.AllStrings)
    module TcpServer = Event.TcpServer (IOType) (Pdu.Blobber)
    module TcpClient = Event.TcpClient (IOType) (Pdu.Blobber)

    type t =
        (* We call 'out' the direction from client to server
         * and 'in' the direction from server to client, ie. we adopt
         * the client perspective. *)
        { listen_to : string ;
          forward_to : Server.t ;
          mutable config : Config.t * Config.t (* in, out *) ;
          mutable condition : Condition.t * Condition.t ;
          mutable queue : IOType.write_cmd Queue.t * IOType.write_cmd Queue.t (* next things to write *) }

    let print fmt t =
        Format.fprintf fmt "%s:%s:%d" t.listen_to t.forward_to.Server.name t.forward_to.Server.port

    let make listen_to host_name host_port =
        { listen_to ;
          forward_to = Server.make host_name (int_of_string host_port) ;
          config = Config.make_ok (), Config.make_ok () ;
          condition = Condition.make_ok (), Condition.make_ok () ;
          queue = Queue.create (), Queue.create () }

    (* String format is similar to ssh port redirection format:
     * listen_to:remote_server:remote_port
     * It is created with a pristine config *)
    let from_string str =
        match String.nsplit str ":" with
        | [ listen_to ; host_name ; host_port ] ->
            make listen_to host_name host_port
        | [ listen_to ; host_port] ->
            make listen_to "localhost" host_port
        | _ ->
            invalid_arg str

    let start ?cnx_timeout t =
        let client_writer_started = ref false in
        let continue = ref true in
        let rec delay_send name w q conf =
            if not (Queue.is_empty q) then (
                let x = Queue.take q in
                L.debug "%s: Writing %s..." name (match x with IOType.Close -> "FIN" | IOType.Write _ -> "a string") ;
                w x ;
                if x = IOType.Close then continue := false
            ) ;
            if !continue then (
                let open Config in
                let d = Random.float (conf.lag.max -. conf.lag.min) +. conf.lag.min in
                L.debug "%s: rescheduling in %gs" name d ;
                Event.pause d (fun () -> delay_send name w q conf)
            ) in
        let server_to_client _to_server = function
            | IOType.Value str ->
                Queue.add (IOType.Write str) (fst t.queue)
            | IOType.Timeout _now -> ()
            | IOType.EndOfFile ->
                L.info "IN: FIN" ;
                Queue.add IOType.Close (fst t.queue) in
        let to_server =
            TcpClient.client ?cnx_timeout t.forward_to.name (string_of_int t.forward_to.port) server_to_client in
        let client_to_server writer v =
            (* We can't know the to_client writer before some client actually connect
             * (note: we allow only one). So we save it here. *)
            if not !client_writer_started then (
                L.debug "Initializing client writer" ;
                client_writer_started := true ;
                delay_send "in" writer (fst t.queue) (fst t.config)
            ) ;
            match v with
            | IOType.Value str ->
                Queue.add (IOType.Write str) (snd t.queue)
            | IOType.Timeout _now -> ()
            | IOType.EndOfFile ->
                L.info "OUT: FIN" ;
                Queue.add IOType.Close (snd t.queue) in
        let shutdown = TcpServer.serve ?cnx_timeout ~max_accepted:1 t.listen_to client_to_server in
        delay_send "out" to_server (snd t.queue) (snd t.config) ;
        shutdown
end

let () =
    let open Cmdliner in
    let link_parser str =
        try `Ok (Link.from_string str)
        with (Invalid_argument s) -> `Error s in
    let link_converter = link_parser, Link.print in
    let setting_parser str =
        try `Ok (Setting.from_string str)
        with (Failure s) -> `Error s in
    let setting_converter = setting_parser, Setting.print in
    let no_setting = Setting.make_ok () in
    let lag =
        let doc = "lag" in
        Arg.(value (opt setting_converter no_setting (info ~doc ["lag"]))) in
    let loss =
        let doc = "loss" in
        Arg.(value (opt setting_converter no_setting (info ~doc ["loss"]))) in
    let fuzzing =
        let doc = "fuzzing" in
        Arg.(value (opt setting_converter no_setting (info ~doc ["fuzz"]))) in
    let links =
        let doc = "port forward description (a la SSH: local_port:remote_host:remote_port)" in
        Arg.(required (pos 0 (some link_converter) None (info ~doc ~docv:"LINK" []))) in
    let start_all link lag loss fuzzing =
        let config = Config.make lag loss fuzzing in
        link.Link.config <- config, config ;
        link.Link.condition <- Condition.make config, Condition.make config ;
        let links = [link] in
        List.map Link.start links in
    match Term.(eval (pure start_all $ links $ lag $ loss $ fuzzing, info "Forward TCP traffic")) with
    | `Error _ -> exit 1
    | `Ok _shutdowns ->
        Event.loop () ;
        exit 0
    | `Version | `Help -> exit 0

