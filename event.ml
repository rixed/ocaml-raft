open Batteries
type fd = Unix.file_descr

type monitored_files = fd list (* readables *)
                     * fd list (* writables *)
                     * fd list (* exceptional conditions *)

type handler = { register_files : monitored_files -> monitored_files ;
                  process_files : handler -> monitored_files -> unit }

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
    handlers := handler :: !handlers

let unregister handler =
    handlers := List.filter ((!=) handler) !handlers

module type IO_Type = sig type t_read type t_write end

(* Buffered reader/writer of marshaled values of type IO_Type.t_read/IO_Type.t_write *)
module BufferedIO (T : IO_Type) =
struct
    let try_write_buf buf fd =
        let str = Buffer.contents buf in
        let sz = Unix.single_write fd str 0 (String.length str) in
        Buffer.clear buf ;
        Buffer.add_substring buf str sz (String.length str - sz)

    type read_result = Value of T.t_read | EndOfFile

    let try_read_value fd buf value_cb writer handler =
        (* Append what can be read from fd into buf ;
           notice that if more than 1500 bytes are available
           then the event loop will call us again at once *)
        let str = String.create 1500 in
        let sz = Unix.read fd str 0 (String.length str) in
        if sz = 0 then (
            value_cb writer EndOfFile ;
            unregister handler
        ) else (
            Buffer.add_substring buf str 0 sz ;
            (* Read one value, apply value_cb to it, then returns the offset of next value.
             * Beware that we may have 0, 1 or more values in rbuf *)
            let rec read_next content ofs =
                let len = String.length content - ofs in
                (* If we have no room for Marshal header then it's too early to read anything *)
                if len < Marshal.header_size ||
                   len < Marshal.header_size + Marshal.data_size content ofs
                then ofs else (
                    (* Otherwise use this header to find out data size
                     * Note: data size does not include header size *)
                    let v : T.t_read = Marshal.from_string content ofs in
                    value_cb writer (Value v) ;
                    read_next content (ofs + len)
                ) in
            let content = Buffer.contents buf in
            Buffer.clear buf ;
            let ofs = read_next content 0 in
            Buffer.add_substring buf content ofs (String.length content - ofs) ;
        )

    let start infd outfd value_cb =
        let inbuf = Buffer.create 2000
        and outbuf = Buffer.create 2000 in
        let writer (v : T.t_write) =
            Marshal.to_string v [] |>
            Buffer.add_string outbuf in
        let register_files (rfiles, wfiles, efiles) =
            let buffer_is_empty b = Buffer.length b = 0 in
            infd :: rfiles,
            (if buffer_is_empty outbuf then wfiles else outfd :: wfiles),
            efiles in
        let process_files handler (rfiles, wfiles, _) =
            if List.mem infd rfiles then
                try_read_value infd inbuf value_cb writer handler ;
            if List.mem outfd wfiles then
                try_write_buf outbuf outfd in
        register { register_files ; process_files } ;
        (* Return the writer function *)
        writer
end

module TcpTools =
struct
    let close fd =
        Log.debug "Closing TCP connection" ;
        Unix.close fd
end

module TcpClient (T : IO_Type) :
sig
    (* [client "server.com" "http" reader] connect to server.com:80 and will send all read value to [reader]
     * function. The returned value is the writer function. *)
    val client : string -> string -> ((T.t_write -> unit) -> T.t_read -> unit) -> (T.t_write -> unit)
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

    let client host service value_cb =
        try let fd = connect host service in
            let my_value_cb writer = function
                | BIO.EndOfFile ->
                    TcpTools.close fd
                | BIO.Value v ->
                    value_cb writer v
            in
            BIO.start fd fd my_value_cb
        with Not_found ->
            failwith ("Cannot connect to "^ host ^":"^ service)
end

module TcpServer (T : IO_Type) :
sig
    (* [serve service callback] listen on port [service] and serve each query with [callback] *)
    val serve : string -> ((T.t_write -> unit) -> T.t_read -> unit) -> unit
end =
struct
    module BIO = BufferedIO (T)

    let listen service =
        let open Unix in
        let se = getservbyname service "tcp" in
        Log.debug "Listening on %s:%s(%d)" se.s_proto se.s_name se.s_port ;
        let sock = socket PF_INET SOCK_STREAM 0 in
        Unix.listen sock 5 ;
        sock

    let serve service value_cb =
        try let listen_fd = listen service in
            let process_client client_fd writer = function
                | BIO.EndOfFile ->
                    TcpTools.close client_fd
                | BIO.Value v ->
                    value_cb writer v in
            let process_listen _writer _v = (* the listened socket will become readable on new SYNs *)
                let client_fd, _sock_addr = Unix.accept listen_fd in
                BIO.start client_fd client_fd (process_client client_fd)
            in
            BIO.start listen_fd listen_fd process_listen |> ignore (* we don't write in the listen socket *)
        with Not_found ->
            failwith ("Cannot listen to "^ service)

end

