open Batteries
type fd = Unix.file_descr

let debug = false

type monitored_files = fd list (* readables *)
                     * fd list (* writables *)
                     * fd list (* exceptional conditions *)

type handler = { register_files : monitored_files -> monitored_files ;
                  process_files : handler (* so we can unregister *) -> monitored_files -> unit }

let select_once handlers =
    let collect_all_monitored_files handlers =
        List.fold_left (fun files handler ->
            handler.register_files files)
            ([],[],[]) handlers
    and process_all_monitored_files handlers files =
        List.iter (fun handler -> handler.process_files handler files) handlers in

    let open Unix in
    let rfiles, wfiles, efiles = collect_all_monitored_files handlers in
    (* Notice: we timeout the select after 1s so that handlers have a chance to implement timeouting *)
    try let changed_files = select rfiles wfiles efiles 1. in
        let ll = List.length in let l (a,b,c) = ll a + ll b + ll c in
        if debug then Log.debug "selected %d files out of %d" (l changed_files) (l (rfiles,wfiles,efiles)) ;
        process_all_monitored_files handlers changed_files
    with
        | Unix_error (EINTR,_,_) -> ()

let handlers = ref []

let loop () =
    if debug then Log.debug "Entering event loop" ;
    while !handlers != [] do
        select_once !handlers
    done
let register handler =
    handlers := handler :: !handlers ;
    if debug then Log.debug "Registering a new handler, now got %d" (List.length !handlers)

let unregister handler =
    handlers := List.filter ((!=) handler) !handlers ;
    if debug then Log.debug "Unregistering a handler, now got %d" (List.length !handlers)

module type BaseIOType =
sig
    type t_read
    type t_write
end

module type IOType =
sig
    include BaseIOType
    type read_result = Value of t_read | EndOfFile
    type write_cmd = Write of t_write | Close
end

module MakeIOType (B : BaseIOType) : IOType with type t_read = B.t_read and type t_write = B.t_write =
struct
    include B
    type read_result = Value of t_read | EndOfFile
    type write_cmd = Write of t_write | Close
end

module MakeIOTypeRev (B : BaseIOType) : IOType with type t_read = B.t_write and type t_write = B.t_read =
struct
    module BaseIOTypeRev =
    struct
        type t_read = B.t_write
        type t_write = B.t_read
    end
    include MakeIOType (BaseIOTypeRev)
end

(* Buffered reader/writer of marshaled values of type IOType.t_read/IOType.t_write. *)
module BufferedIO (T : IOType) =
struct
    let try_write_buf buf fd =
        let str = Buffer.contents buf in
        if debug then Log.debug "writing %d bytes ('%s')" (String.length str) str ;
        let sz = Unix.single_write fd str 0 (String.length str) in
        Buffer.clear buf ;
        Buffer.add_substring buf str sz (String.length str - sz)

    type todo = Nope | ToDo | Done (* for deferred closes *)
    let try_read_value fd buf value_cb writer handler close_out closed_in =
        (* Append what can be read from fd into buf ;
           notice that if more than 1500 bytes are available
           then the event loop will call us again at once *)
        let str = String.create 1500 in
        let sz = Unix.read fd str 0 (String.length str) in
        if debug then Log.debug "Read %d bytes from file" sz ;
        if sz = 0 then (
            if debug then Log.debug "infd is closed by peer" ;
            closed_in := true ;
            value_cb writer T.EndOfFile ;
            if close_out = Done then unregister handler
        ) else (
            Buffer.add_substring buf str 0 sz ;
            (* Read one value, apply value_cb to it, then returns the offset of next value.
             * Beware that we may have 0, 1 or more values in rbuf *)
            let rec read_next content ofs =
                let len = String.length content - ofs in
                if debug then Log.debug "Still %d bytes to read from buffer" len ;
                (* If we have no room for Marshal header then it's too early to read anything *)
                if len < Marshal.header_size then ofs else
                let value_len = Marshal.header_size + Marshal.data_size content ofs in
                if len < value_len then ofs else (
                    (* Otherwise use this header to find out data size
                     * Note: data size does not include header size *)
                    let v : T.t_read = Marshal.from_string content ofs in
                    value_cb writer (T.Value v) ;
                    read_next content (ofs + value_len)
                ) in
            let content = Buffer.contents buf in
            Buffer.clear buf ;
            let ofs = read_next content 0 in
            Buffer.add_substring buf content ofs (String.length content - ofs) ;
        )

    let start ?timeout infd outfd value_cb =
        let last_used = ref 0. in (* for timeouting *)
        let reset_timeout_and f x =
            last_used := Unix.time () ;
            f x in
        let inbuf = Buffer.create 2000
        and outbuf = Buffer.create 2000
        and close_out = ref Nope and closed_in = ref false in
        let writer = function
            | T.Write v ->
                Marshal.to_string v [] |>
                Buffer.add_string outbuf
            | T.Close ->
                assert (!close_out = Nope) ;
                close_out := ToDo in
        let writer = reset_timeout_and writer in
        let buffer_is_empty b = Buffer.length b = 0 in
        let register_files (rfiles, wfiles, efiles) =
            infd :: rfiles,
            (if buffer_is_empty outbuf && !close_out <> ToDo then wfiles else outfd :: wfiles),
            efiles in
        let process_files handler (rfiles, wfiles, _) =
            if List.mem infd rfiles then
                try_read_value infd inbuf (reset_timeout_and value_cb) writer handler !close_out closed_in ;
            if List.mem outfd wfiles then (
                if not (buffer_is_empty outbuf) then
                    try_write_buf outbuf outfd) ;
            Option.may (fun timeout ->
                if Unix.time() -. !last_used > timeout then (
                    (* Pretend we received an EndOfFile *)
                    if not !closed_in then (
                        Unix.(shutdown infd SHUTDOWN_RECEIVE) ;
                        value_cb writer T.EndOfFile ;
                        closed_in := true)))
                timeout ;
            if !close_out = ToDo then (
                if debug then Log.debug "Closing outfd" ;
                Unix.(shutdown outfd SHUTDOWN_SEND) ;
                close_out := Done ;
                if !closed_in then unregister handler)
            in
        register { register_files ; process_files } ;
        (* Return the writer function *)
        writer
end

module TcpClient (T : IOType) :
sig
    (* [client "server.com" "http" reader] connect to server.com:80 and will send all read value to [reader]
     * function. The returned value is the writer function.
     * Notice that the reader is a callback, so this is different (and more complex) than the function call
     * abstraction. The advantage is that the main thread can do a remote call and proceed to something else
     * instead of being forced to wait for the response (event driven design). This also allows 0 or more than
     * 1 return values.
     * When timeouting, [reader] will be called with [EndOfFile] and input stream will be closed. You must
     * still close output stream if you want to close the socket for good. *)
    val client : ?timeout:float -> string -> string -> ((T.write_cmd -> unit) -> T.read_result -> unit) -> (T.write_cmd -> unit)
end =
struct
    module BIO = BufferedIO (T)

    let connect host service =
        let open Unix in
        getaddrinfo host service [AI_SOCKTYPE SOCK_STREAM ; AI_CANONNAME ] |>
        List.find_map (fun ai ->
            if debug then Log.debug "Trying to connect to %s:%s" ai.ai_canonname service ;
            try let sock = socket ai.ai_family ai.ai_socktype ai.ai_protocol in
                Unix.connect sock ai.ai_addr ;
                Some sock
            with exn ->
                if debug then Log.debug "Cannot connect: %s" (Printexc.to_string exn) ;
                None)

    let client ?timeout host service buf_reader =
        try let fd = connect host service in
            BIO.start ?timeout fd fd buf_reader
        with Not_found ->
            failwith ("Cannot connect to "^ host ^":"^ service)
end

module TcpServer (T : IOType) :
sig
    (* [serve service callback] listen on port [service] and serve each query with [callback].
     * A shutdown function is returned that will stop the server from accepting new connections. *)
    val serve : ?timeout:float -> ?max_accepted:int -> string -> ((T.write_cmd -> unit) -> T.read_result -> unit) -> (unit -> unit)
end =
struct
    module BIO = BufferedIO (T)

    let listen service =
        let open Unix in
        match getaddrinfo "" service [AI_SOCKTYPE SOCK_STREAM; AI_PASSIVE;] with
        | ai::_ ->
            if debug then Log.debug "Listening on %s" service ;
            let sock = socket PF_INET SOCK_STREAM 0 in
            setsockopt sock SO_REUSEADDR true ;
            setsockopt sock SO_KEEPALIVE true ;
            bind sock ai.ai_addr ;
            listen sock 5 ;
            sock
        | [] ->
            failwith ("Cannot listen to "^ service)

    let serve ?timeout ?max_accepted service value_cb =
        let accepted = ref 0 in
        Option.may (fun n -> assert (n > 0)) max_accepted ;
        let listen_fd = listen service in
        let rec stop_listening () =
            if debug then Log.debug "Closing listening socket" ;
            Unix.close listen_fd ;
            unregister handler
        (* We need a special kind of event handler to handle the listener fd:
         * one that accept when it's readable and that never write. *)
        and register_files (rfiles, wfiles, efiles) =
            listen_fd :: rfiles, wfiles, efiles
        and process_files _handler (rfiles, _, _) =
            if List.mem listen_fd rfiles then (
                if debug then Log.debug "Reading a SYN, accepting cnx" ;
                let client_fd, _sock_addr = Unix.accept listen_fd in
                incr accepted ;
                (match max_accepted with
                    | Some n when !accepted >= n -> stop_listening ()
                    | _ -> ()) ;
                let _buf_writer = BIO.start ?timeout client_fd client_fd value_cb in
                ()
            )
        and handler = { register_files ; process_files } in
        register handler ;
        stop_listening
end

