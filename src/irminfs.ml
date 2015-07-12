(*
 * Copyright (c) 2015 David Sheets <sheets@alum.mit.edu>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)

open Lwt.Infix

module Stat = Unix_sys_stat

module Dir = struct
  type t = Handles.Unix_dir.t * string list

  let set_dir_offset = Handles.(function
    | { kind=Dir ((dh,_), path) } as h -> fun off ->
      update h { h with kind=Dir ((dh,off),path) }
    | { kind=File _ } -> raise Unix.(Unix_error (ENOTDIR,"",""))
  )
  let close ((h, _),_) = Unix.closedir h
end

module H = Handles.Make(Dir)(Handles.Unix_file)

module FSPath = struct
  include Nodes.Path
  let to_string = function
    | [] -> "."
    | p  -> to_string p
end

module N = Nodes.Make(FSPath)

module Path  = Irminfs_path
module Ino   = Irminfs_ino
module Node  = Irminfs_node
module Bundle= Irminfs_bundle
module Store = Irmin.Basic(Irmin_unix.Irmin_http.Make)(Node)
module Space = Irminfs_space.Make(Store)
module View  = Space.View
module Trans = Irminfs_trans

type state = {
  nodes   : N.t;
  handles : H.t;
  agents  : Agent_handler.t;
  irmin   : Trans.t -> Store.t;
}
type t = state

(* TODO: check N.get raising Not_found *)
(* TODO: check uid/gid rights *)

let string_of_nodeid nodeid st = N.string_of_id st.nodes nodeid

let uint64_of_int64 = Unsigned.UInt64.of_int64
let uint32_of_uint64 x = Unsigned.(UInt32.of_int (UInt64.to_int x))

let make uri =
  let irmin_config = Irmin_http.config uri in
  {
    nodes = N.create [];
    handles = H.create ();
    agents = Agent_handler.create ();
    irmin = Lwt_main.run (Store.create irmin_config Trans.task);
  }

module Linux_7_8(In : In.LINUX_7_8)(Out : Out.LINUX_7_8)
  : Profuse.FULL with type t = state and module In = In =
struct
  module In = In
  type t = state

  module Support = Profuse.Linux_7_8(In)(Out)

  let string_of_state req st =
    Printf.sprintf "Nodes: %s" (N.to_string st.nodes)

  let negotiate_mount pkt req st =
    ignore (Unix.umask 0o000);
    Support.negotiate_mount pkt req st

  let enosys = Support.enosys
  let nodeid = Support.nodeid

  let store_attr_of_path taskf path = Stat.(Stat.(
    let plist = List.rev path in
    let store = taskf (Trans.lookup plist) in
    match Lwt_main.run Space.(read Ino store plist) with
    | Some ({ Ino.T.ino;
              atime; mtime; ctime;
              mode; links;
              uid; gid;
            } as node) ->
      let nlink =
        Unsigned.UInt32.of_int (1 + Ino.PathSet.cardinal links)
      in
      Struct.Linux_7_8.Attr.store
        ~ino
        ~size:(Space.size store plist node)
        ~blocks:0_L (* TODO: check *)
        ~atime:(uint64_of_int64 atime)
        ~atimensec:(Unsigned.UInt32.of_int 0)
        ~mtime:(uint64_of_int64 mtime)
        ~mtimensec:(Unsigned.UInt32.of_int 0)
        ~ctime:(uint64_of_int64 ctime)
        ~ctimensec:(Unsigned.UInt32.of_int 0)
        ~mode:(Int32.of_int mode)
        ~nlink
        ~uid
        ~gid
        ~rdev:(Unsigned.UInt32.of_int32 0_l) (* TODO: assumes regular file *)
    | None ->
      let s = lstat (FSPath.to_string path) in
      Struct.Linux_7_8.Attr.store
        ~ino:(Unsigned.UInt64.to_int64 (ino_int s))
        ~size:(size_int s)
        ~blocks:(blocks_int s)
        ~atime:(uint64_of_int64 (atime_int s))
        ~atimensec:(atimensec_int s)
        ~mtime:(uint64_of_int64 (mtime_int s))
        ~mtimensec:(mtimensec_int s)
        ~ctime:(uint64_of_int64 (ctime_int s))
        ~ctimensec:(ctimensec_int s)
        ~mode:(Unsigned.UInt32.to_int32 (mode_int s))
        ~nlink:(uint32_of_uint64 (nlink_int s))
        ~uid:(Unsigned.UInt32.to_int32 (uid_int s))
        ~gid:(Unsigned.UInt32.to_int32 (gid_int s))
        ~rdev:(uint32_of_uint64 (rdev_int s))
  ))

  let getattr req st = Out.(
    try
      let { Nodes.data = path } = N.get st.nodes (nodeid req) in
      write_reply req
        (Attr.create ~attr_valid:0L ~attr_valid_nsec:0l
           ~store_attr:(store_attr_of_path st.irmin path));
      st
    with Not_found ->
      (* TODO: log? *)
      Printf.eprintf "getattr not found\n%!";
      write_error req Unix.ENOENT;
      st
  )

  let opendir op req st = Out.(
    try
      let { nodes } = st in
      let { Nodes.data } = N.get nodes (nodeid req) in
      let path = FSPath.to_string data in
      let dir = Unix.opendir path in
      let h = H.(alloc st.handles (Handles.Dir ((dir, 0),List.rev data))) in
      write_reply req (Open.create ~fh:h.Handles.id ~open_flags:0l);
      (* TODO: open_flags?? *)
      st
    with Not_found ->
      (* TODO: log? *)
      Printf.eprintf "opendir not found\n%!";
      write_error req Unix.ENOENT;
      st
  )

  let forget n req st = Out.(
    try
      let node = N.get st.nodes (nodeid req) in
      N.forget node n;
      st
    with Not_found ->
      (* TODO: FORGET is a non-returning command. log? *)
      Printf.eprintf "forget not found\n%!";
      st
  )

  let store_entry taskf =
    Support.store_entry (fun node -> store_attr_of_path taskf node.Nodes.data)
  let respond_with_entry taskf =
    Support.respond_with_entry (fun node ->
      store_attr_of_path taskf node.Nodes.data
    )

  let lookup name req st =
    let parent = N.get st.nodes (nodeid req) in
    respond_with_entry st.irmin (N.lookup parent name) req;
    st

  let readdir r req st = Out.(
    let host = Fuse.(req.chan.host) in
    let req_off = Int64.to_int (Ctypes.getf r In.Read.offset) in
    let fh = Ctypes.getf r In.Read.fh in
    H.with_dir_fd st.handles fh (fun h ((dir,off), path) ->
      let store = st.irmin (Trans.readdir path) in
      let paths = Lwt_main.run (Space.list_nodes store path) in
      let unix_start = List.length paths in
      if req_off < unix_start
      then
        let rec seek list = function
          | 0 -> list
          | n -> seek (List.tl list) (n - 1)
        in
        let paths = seek paths req_off in
        let name = Path.hd (List.hd paths) in
        print_endline ("readdir return "^name);
        let off = req_off + 1 in
        Dir.set_dir_offset h off;
        (* TODO: optimize, fix inode *)
        let entry = [off, 1_L, name, Unix_dirent.File_kind.DT_UNKNOWN] in
        write_reply req (Out.Dirent.of_list ~host entry 0)
      else

        let rec seek dir off =
          if off < req_off then (ignore (Unix.readdir dir); seek dir (off + 1))
          else if off > req_off then (Unix.rewinddir dir; seek dir unix_start)
          else off
        in

        let off = seek dir off in
        assert (off = req_off);
        write_reply req
          (Out.Dirent.of_list ~host begin
             try
               let { Unix_dirent.Dirent.name; kind; ino } =
                 Unix_dirent.readdir dir
               in
               let off = off + 1 in
               Dir.set_dir_offset h off;
               [off, ino, name, kind]
             with End_of_file ->
               []
           end 0)
    );
    st
  )

  (* Can raise Unix.Unix_error *)
  let readlink req st =
    let node = N.get st.nodes (nodeid req) in
    (* errors caught by our caller *)
    let path = List.rev node.Nodes.data in

    let store = st.irmin (Trans.lookup path) in
    let target = Node.(Ino.(T.(match Lwt_main.run (
      Space.(read Ino store path)
    ) with
    | Some ({ kind = Kind.Symlink target }) -> target
    | Some _ ->
      let path = FSPath.to_string node.Nodes.data in
      raise Unix.(Unix_error (EINVAL, "readlink", path))
    | None ->
      let path = FSPath.to_string node.Nodes.data in
      Unix.readlink path
    )))
    in

    Out.(write_reply req (Readlink.create ~target));
    st

  let open_ op req st = Out.(
    try
      let { nodes; handles; agents } = st in
      let { Nodes.data } = N.get nodes (nodeid req) in
      let path = FSPath.to_string data in
      let uid = Ctypes.getf req.Fuse.hdr In.Hdr.uid in
      let gid = Ctypes.getf req.Fuse.hdr In.Hdr.gid in
      let mode = Ctypes.getf op In.Open.mode in (* TODO: is only file_perm? *)
      let flags = Ctypes.getf op In.Open.flags in
      let phost = Fuse.(req.chan.host.unix_fcntl.Unix_fcntl.oflags) in
      let flags = Unix_fcntl.Oflags.(
        List.rev_map to_open_flag_exn (of_code ~host:phost flags)
      ) in
      let file = agents.Agent_handler.open_ ~uid ~gid path flags mode in
      let kind = Unix.((fstat file).st_kind) in
      let h = H.(alloc handles (Handles.File (file, kind))) in
      Out.(write_reply req (Open.create ~fh:h.Handles.id ~open_flags:0l));
      (* TODO: flags *)
      st
    with Not_found ->
      (* TODO: log? *)
      write_error req Unix.ENOENT; st
  )

  let read r req st =
    let fh = Ctypes.getf r In.Read.fh in
    let offset = Ctypes.getf r In.Read.offset in
    let size = Ctypes.getf r In.Read.size in
    H.with_file_fd st.handles fh (fun _h fd _k -> Out.(
      write_reply req
        (Read.create ~size ~data_fn:(fun buf ->
          let ptr = Ctypes.(to_voidp (bigarray_start array1 buf)) in
          Unix_unistd.pread fd ptr size offset
         )))
    );
    st

  (* TODO: anything? *)
  let flush f req st = Out.write_ack req; st

  (* TODO: flags? *)
  let release r req st =
    try
      H.(free (get st.handles (Ctypes.getf r In.Release.fh)));
      Out.write_ack req;
      st
    with Not_found ->
      Out.write_error req Unix.EBADF; st

  (* TODO: distinguish? *)
  let releasedir = release

  (* Can raise Unix.Unix_error *)
  let symlink name target req st = Out.(
    let ({ Nodes.data } as pnode) = N.get st.nodes (nodeid req) in
    let path = List.rev (name::data) in

    let store = st.irmin Trans.({
      call = Call.Symlink (target, path)
    }) in
    let uid = In.(Ctypes.getf req.Fuse.hdr Hdr.uid) in
    let gid = In.(Ctypes.getf req.Fuse.hdr Hdr.gid) in
    let host = Fuse.(req.chan.host.unix_sys_stat.Unix_sys_stat.mode) in
    Lwt_main.run (
      (* TODO: check existence *)
      Space.(create store path Ino.(T.({ Bundle.ino={
        ino = Int64.of_int (Ino.next_ino ());
        mode = Stat.Mode.to_code ~host (Unix.S_LNK, 0o777);
        uid; gid;
        atime = 0_L; (* TODO: FIXME *)
        mtime = 0_L; (* TODO: FIXME *)
        ctime = 0_L; (* TODO: FIXME *)
        links = PathSet.empty;
        kind = Kind.Symlink target;
      };
        dat = None;
      })))
      >>= function
      | true -> Lwt.return_unit
      | false -> Lwt.fail Unix.(Unix_error (EEXIST, "symlink", ""))
    );

    lookup name req st (* TODO: still increment lookups? *)
  )

  (* Can raise Unix.Unix_error *)
  let rename r src dest req st = Out.(
    let { Nodes.data } = N.get st.nodes (nodeid req) in
    let newdir = N.get st.nodes (Ctypes.getf r In.Rename.newdir) in
    let spath = List.rev (src::data) in
    let dpath = List.rev (dest::newdir.Nodes.data) in
    let store = st.irmin (Trans.rename spath dpath) in

    Lwt_main.run begin
      View.of_path store Path.empty
      >>= fun root ->
      Space.bundle root spath
      >>= function
      | None ->
        let path = FSPath.to_string data in
        let newpath = FSPath.to_string newdir.Nodes.data in
        (* errors caught by our caller *)
        (* TODO: lift and remove with whiteout, don't modify underfs *)
        Lwt_unix.rename (Filename.concat path src) (Filename.concat newpath dest)
      | Some { Bundle.ino; dat } ->
        begin
          View.update Space.Ino root dpath ino
          >>= fun () ->
          View.remove root (Path.ino spath)
          >>= fun () -> match dat with
          | None -> View.merge_path store Path.empty root
          | Some dat ->
            View.update Space.Dat root dpath dat
            >>= fun () ->
            View.remove root (Path.dat spath)
            >>= fun () ->
            View.merge_path store Path.empty root
        end >|= function
        | `Ok () -> ()
        | `Conflict _ -> raise Unix.(Unix_error (EBUSY, "rename", ""))
    end;
    write_ack req;
    st
  )

  (* Can raise Unix.Unix_error *)
  let unlink name req st = Out.(
    let { Nodes.data } = N.get st.nodes (nodeid req) in
    let path = List.rev (name::data) in
    let store = st.irmin (Trans.unlink path) in

    Lwt_main.run begin
      Space.delete store path
      >>= function
      | true -> Lwt.return_unit
      | false ->
        (* TODO: remove with whiteout. don't modify underfs *)
        let path = Filename.concat (FSPath.to_string data) name in
        (* errors caught by our caller *)
        Lwt_unix.unlink path
    end;

    write_ack req;
    st
  )

  (* Can raise Unix.Unix_error *)
  let rmdir name req st = Out.(
    let { agents } = st in
    let { Nodes.data } = N.get st.nodes (nodeid req) in
    let path = List.rev (name::data) in
    let store = st.irmin (Trans.rmdir path) in
    match Lwt_main.run (Space.list_nodes store path) with
    | [] ->
      let path = FSPath.to_string data in
      let uid = Ctypes.getf req.Fuse.hdr In.Hdr.uid in
      let gid = Ctypes.getf req.Fuse.hdr In.Hdr.gid in
      let path = Filename.concat path name in
      (* errors caught by our caller *)
      agents.Agent_handler.rmdir ~uid ~gid path;
      write_ack req;
      st
    | _::_ -> raise Unix.(Unix_error (ENOTEMPTY,"rmdir",name))
  )

  (* TODO: don't lie *)
  let statfs req st =
    let module Statfs = Struct_common.Kstatfs in
    let one_gb = 1 lsl 30 in
    let bsize = 512 in
    let blocks = uint64_of_int64 (Int64.of_int (one_gb / bsize)) in
    let write req =
      let pkt = Out_common.Hdr.make req Statfs.t in
      Statfs.store
        ~blocks
        ~bfree:blocks
        ~bavail:blocks
        ~files:blocks
        ~ffree:blocks
        ~bsize:(Unsigned.UInt32.of_int bsize)
        ~namelen:(Unsigned.UInt32.of_int 255)
        ~frsize:(Unsigned.UInt32.of_int bsize)
        pkt;
      Ctypes.(CArray.from_ptr
                (coerce (ptr Statfs.t) (ptr char) (addr pkt)) (sizeof Statfs.t))
    in
    Out.write_reply req write;
    st

  (* TODO: do *)
  let fsync _f = enosys

  (* Can raise Unix.Unix_error *)
  (* TODO: write flags? *)
  let write w req st =
    let fh = Ctypes.getf w In.Write.fh in
    let offset = Ctypes.getf w In.Write.offset in
    let size = Ctypes.getf w In.Write.size in
    H.with_file_fd st.handles fh (fun _h fd _k -> Out.(
      let data = Ctypes.(to_voidp (CArray.start (getf w In.Write.data))) in
      (* errors caught by our caller *)
      let size = Unix_unistd.pwrite fd data size offset in
      write_reply req (Write.create ~size)
    ));
    st

  (* Can raise Unix.Unix_error *)
  let link l name req st =
    let { Nodes.data } = N.get st.nodes (nodeid req) in
    let path = Filename.concat (FSPath.to_string data) name in
    let oldnode = N.get st.nodes (Ctypes.getf l In.Link.oldnodeid) in
    let oldpath = FSPath.to_string oldnode.Nodes.data in
    (* errors caught by our caller *)
    Unix.link oldpath path;
    lookup name req st (* TODO: still increment lookups? *)

  (* TODO: do *)
  let getxattr _g = enosys

  (* TODO: do *)
  let setxattr _s = enosys

  (* TODO: do *)
  let listxattr _g = enosys

  (* TODO: do *)
  let removexattr _name = enosys

  let access a req st =
    let { agents } = st in
    let { Nodes.data } = N.get st.nodes (nodeid req) in
    let path = FSPath.to_string data in
    let uid = Ctypes.getf req.Fuse.hdr In.Hdr.uid in
    let gid = Ctypes.getf req.Fuse.hdr In.Hdr.gid in
    let code = Ctypes.getf a In.Access.mask in
    let phost = Fuse.(req.chan.host.unix_unistd.Unix_unistd.access) in
    let perms = Unix_unistd.Access.(of_code ~host:phost code) in
    agents.Agent_handler.access ~uid ~gid path perms;
    Out.write_ack req;
    st

  let create c name req st = Out.(
    try
      let { nodes; handles } = st in
      let ({ Nodes.data } as pnode) = N.get nodes (nodeid req) in
      let path = FSPath.to_string data in
      let mode = Ctypes.getf c In.Create.mode in (* TODO: is only file_perm? *)
      let flags = Ctypes.getf c In.Create.flags in
      let phost = Fuse.(req.chan.host.unix_fcntl.Unix_fcntl.oflags) in
      let flags = Unix_fcntl.Oflags.(
        List.rev_map to_open_flag_exn (of_code ~host:phost flags)
      ) in
      let path = Filename.concat path name in
      let file = Unix.(
        openfile path (O_WRONLY::O_CREAT::O_TRUNC::flags) (Int32.to_int mode)
      ) in
      let kind = Unix.((Unix.fstat file).st_kind) in
      let h = H.(alloc handles (Handles.File (file, kind))) in
      write_reply req
        (Create.create
           ~store_entry:(store_entry st.irmin (N.lookup pnode name))
           ~store_open:(Open.store ~fh:h.Handles.id ~open_flags:0l));
      (* TODO: flags *)
      st
    with Not_found ->
      (* TODO: log? *)
      write_error req Unix.ENOENT; st
  )

  let mknod m name req st =
    let { agents } = st in
    let ({ Nodes.data } as pnode) = N.get st.nodes (nodeid req) in
    let path = FSPath.to_string data in
    let uid = Ctypes.getf req.Fuse.hdr In.Hdr.uid in
    let gid = Ctypes.getf req.Fuse.hdr In.Hdr.gid in
    let path = Filename.concat path name in
    let mode = Ctypes.getf m In.Mknod.mode in
    let rdev = Ctypes.getf m In.Mknod.rdev in (* TODO: use this? *)
    (* TODO: translate mode and dev from client host rep to local host rep *)
    (* TODO: dev_t is usually 64-bit but rdev is 32-bit. translate how? *)
    (* TODO: regular -> open with O_CREAT | O_EXCL | O_WRONLY for compat? *)
    (* TODO: fifo -> mkfifo for compat? *)
    agents.Agent_handler.mknod ~uid ~gid path mode
      (Unsigned.UInt32.to_int32 rdev);
    respond_with_entry st.irmin (N.lookup pnode name) req;
    st

  let mkdir m name req st =
    let { agents } = st in
    let ({ Nodes.data } as pnode) = N.get st.nodes (nodeid req) in
    let path = FSPath.to_string data in
    let uid = Ctypes.getf req.Fuse.hdr In.Hdr.uid in
    let gid = Ctypes.getf req.Fuse.hdr In.Hdr.gid in
    let path = Filename.concat path name in
    let mode = Ctypes.getf m In.Mkdir.mode in
    agents.Agent_handler.mkdir ~uid ~gid path mode;
    respond_with_entry st.irmin (N.lookup pnode name) req;
    st

  (* TODO: do *)
  let fsyncdir _f = enosys

  (* TODO: do *)
  let getlk _lk = enosys

  (* TODO: do *)
  let setlk _lk = enosys

  (* TODO: do *)
  let setlkw _lk = enosys

  (* TODO: do *)
  let interrupt _i = enosys

  (* TODO: do *)
  let bmap _b = enosys

  let destroy _req _st = ()

  let setattr s req st = In.Setattr.(
    let valid = Ctypes.getf s valid in
    begin
      if Valid.(is_set valid handle)
      then H.with_file_fd st.handles (Ctypes.getf s fh) (fun _h fd k ->
        (if Valid.(is_set valid mode)
         then
            let mode = Ctypes.getf s mode in
            let phost = Fuse.(req.chan.host.unix_sys_stat.Unix_sys_stat.mode) in
            let (kind,perm) = Stat.Mode.(
              of_code_exn ~host:phost (Int32.to_int mode)
            ) in
            assert (kind = k); (* TODO: ???!!! *)
            Unix.fchmod fd perm);
        (let set_uid = Valid.(is_set valid uid) in
         let set_gid = Valid.(is_set valid gid) in
         if set_uid || set_gid
         then Unix.fchown fd
           (if set_uid then Ctypes.getf s uid else -1)
           (if set_gid then Ctypes.getf s gid else -1)
        );
        (if Valid.(is_set valid size)
         then Unix.LargeFile.ftruncate fd (Ctypes.getf s size));
        (if Valid.(is_set valid atime) (* TODO: do *)
         then prerr_endline "setting atime");
        (if Valid.(is_set valid mtime) (* TODO: do *)
         then prerr_endline "setting mtime");
      )
      else
        let { Nodes.data } = N.get st.nodes (nodeid req) in
        let path = FSPath.to_string data in
        (if Valid.(is_set valid mode)
         then
            let mode = Ctypes.getf s mode in
            let phost = Fuse.(req.chan.host.unix_sys_stat.Unix_sys_stat.mode) in
            let (kind,perm) = Stat.Mode.(
              of_code_exn ~host:phost (Int32.to_int mode)
            ) in
            let { Unix.st_kind } = Unix.stat path in
            assert (kind = st_kind); (* TODO: ???!!! *)
            Unix.chmod path perm);
        (let set_uid = Valid.(is_set valid uid) in
         let set_gid = Valid.(is_set valid gid) in
         if set_uid || set_gid
         then Unix.chown path
           (if set_uid then Ctypes.getf s uid else -1)
           (if set_gid then Ctypes.getf s gid else -1)
        );
        (if Valid.(is_set valid size)
         then Unix.LargeFile.truncate path (Ctypes.getf s size)
        );
        (if Valid.(is_set valid atime) (* TODO: do *)
         then prerr_endline "setting atime");
        (if Valid.(is_set valid mtime) (* TODO: do *)
         then prerr_endline "setting mtime");
    end;
    getattr req st
  )
end
