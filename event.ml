open Batteries
type fd = Unix.file_descr

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
    try let changed_files = select rfiles wfiles efiles (-1.) in
        let ll = List.length in let l (a,b,c) = ll a + ll b + ll c in
        Log.debug "selected %d files out of %d" (l changed_files) (l (rfiles,wfiles,efiles)) ;
        process_all_monitored_files handlers changed_files
    with
        | Unix_error (EINTR,_,_) -> ()

let handlers = ref []

let loop () =
    Log.debug "Entering event loop" ;
    while !handlers != [] do
        select_once !handlers
    done
let register handler =
    handlers := handler :: !handlers ;
    Log.debug "Registering a new handler, now got %d" (List.length !handlers)

let unregister handler =
    handlers := List.filter ((!=) handler) !handlers ;
    Log.debug "Unregistering a handler, now got %d" (List.length !handlers)

module type BaseIOType =
sig
    type t_read
    type t_write
    val print_t_write : 'a BatInnerIO.output -> t_write -> unit
    val print_t_read  : 'a BatInnerIO.output -> t_read  -> unit
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
        let print_t_write = B.print_t_read
        let print_t_read = B.print_t_write
    end
    include MakeIOType (BaseIOTypeRev)
end

(* Buffered reader/writer of marshaled values of type IOType.t_read/IOType.t_write. *)
module BufferedIO (T : IOType) =
struct
    let try_write_buf buf fd =
        let str = Buffer.contents buf in
        Log.debug "writing %d bytes ('%s')" (String.length str) str ;
        let sz = Unix.single_write fd str 0 (String.length str) in
        Buffer.clear buf ;
        Buffer.add_substring buf str sz (String.length str - sz)

    let try_read_value fd buf value_cb writer handler close_out closed_in =
        (* Append what can be read from fd into buf ;
           notice that if more than 1500 bytes are available
           then the event loop will call us again at once *)
        let str = String.create 1500 in
        let sz = Unix.read fd str 0 (String.length str) in
        Log.debug "Read %d bytes" sz ;
        if sz = 0 then (
            Log.debug "Closing infd" ;
            Unix.(shutdown fd SHUTDOWN_RECEIVE) ;
            closed_in := true ;
            value_cb writer T.EndOfFile ;
            if close_out = 2 then unregister handler
        ) else (
            Buffer.add_substring buf str 0 sz ;
            (* Read one value, apply value_cb to it, then returns the offset of next value.
             * Beware that we may have 0, 1 or more values in rbuf *)
            let rec read_next content ofs =
                let len = String.length content - ofs in
                Log.debug "still %d bytes to read" len ;
                (* If we have no room for Marshal header then it's too early to read anything *)
                if len < Marshal.header_size then ofs else
                let value_len = Marshal.header_size + Marshal.data_size content ofs in
                if len < value_len then ofs else (
                    (* Otherwise use this header to find out data size
                     * Note: data size does not include header size *)
                    let v : T.t_read = Marshal.from_string content ofs in
                    Log.debug "Read an actual value: '%a'" T.print_t_read v ;
                    value_cb writer (T.Value v) ;
                    read_next content (ofs + value_len)
                ) in
            let content = Buffer.contents buf in
            Buffer.clear buf ;
            let ofs = read_next content 0 in
            Buffer.add_substring buf content ofs (String.length content - ofs) ;
        )

    let start infd outfd value_cb =
        let inbuf = Buffer.create 2000
        and outbuf = Buffer.create 2000
        and close_out = ref 0 and closed_in = ref false in
        let writer = function
            | T.Write v ->
                Log.debug "writing value '%a' to buffer" T.print_t_write v ;
                Marshal.to_string v [] |>
                Buffer.add_string outbuf
            | T.Close ->
                assert (!close_out = 0) ;
                close_out := 1 in
        let buffer_is_empty b = Buffer.length b = 0 in
        let register_files (rfiles, wfiles, efiles) =
            infd :: rfiles,
            (if buffer_is_empty outbuf && !close_out <> 1 then wfiles else outfd :: wfiles),
            efiles in
        let process_files handler (rfiles, wfiles, _) =
            if List.mem infd rfiles then
                try_read_value infd inbuf value_cb writer handler !close_out closed_in ;
            if List.mem outfd wfiles then (
                if not (buffer_is_empty outbuf) then
                    try_write_buf outbuf outfd ;
                if !close_out = 1 then (
                    Log.debug "Closing outfd" ;
                    Unix.(shutdown outfd SHUTDOWN_SEND) ;
                    close_out := 2 ;
                    if !closed_in then unregister handler
                )
            ) in
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
     * instead of being forced to wait for the response (event driven design). This also allow 0 or more than
     * 1 return values. *)
    val client : string -> string -> ((T.write_cmd -> unit) -> T.read_result -> unit) -> (T.write_cmd -> unit)
end =
struct
    module BIO = BufferedIO (T)

    let connect host service =
        let open Unix in
        getaddrinfo host service [AI_SOCKTYPE SOCK_STREAM ; AI_CANONNAME ] |>
        List.find_map (fun ai ->
            Log.debug "Trying to connect to %s:%s" ai.ai_canonname service ;
            try let sock = socket ai.ai_family ai.ai_socktype ai.ai_protocol in
                Unix.connect sock ai.ai_addr ;
                Some sock
            with exn ->
                Log.debug "Cannot connect: %s" (Printexc.to_string exn) ;
                None)

    let client host service buf_reader =
        try let fd = connect host service in
            BIO.start fd fd buf_reader
        with Not_found ->
            failwith ("Cannot connect to "^ host ^":"^ service)
end

module TcpServer (T : IOType) :
sig
    (* [serve service callback] listen on port [service] and serve each query with [callback].
     * A shutdown function is returned that will stop the server from accepting new connections. *)
    val serve : string -> ((T.write_cmd -> unit) -> T.read_result -> unit) -> (unit -> unit)
end =
struct
    module BIO = BufferedIO (T)

    let listen service =
        let open Unix in
        match getaddrinfo "" service [AI_SOCKTYPE SOCK_STREAM; AI_PASSIVE;] with
        | ai::_ ->
            Log.debug "Listening on %s" service ;
            let sock = socket PF_INET SOCK_STREAM 0 in
            Unix.bind sock ai.ai_addr ;
            Unix.listen sock 5 ;
            sock
        | [] ->
            failwith ("Cannot listen to "^ service)

    let serve service value_cb =
        let listen_fd = listen service in
        (* We need a special kind of event handler to handle the listener fd:
         * one that accept when it's readable and that never write. *)
        let register_files (rfiles, wfiles, efiles) =
            listen_fd :: rfiles, wfiles, efiles
        and process_files _handler (rfiles, _, _) =
            if List.mem listen_fd rfiles then (
                Log.debug "Reading a SYN, accepting cnx" ;
                let client_fd, _sock_addr = Unix.accept listen_fd in
                let _buf_writer = BIO.start client_fd client_fd value_cb in
                ()
            ) in
        let handler = { register_files ; process_files } in
        register handler ;
        fun () ->
            Log.debug "Closing listening socket" ;
            Unix.close listen_fd ;
            unregister handler
end

