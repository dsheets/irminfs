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

module Path  = Irminfs_path
module Paths = Path.Set
module Pathm = struct
  include Map.Make(Path)

  let of_list pairs = List.fold_left (fun m (k,v) -> add k v m) empty pairs
end

let split = Stringext.split ~on:'/'
let concat = String.concat "/"

module Kind = struct
  type t =
    | Set    (** set operations that are commutative mutations *)
    | Create
    | Read
    | Update
    | Delete

  let of_json = function
    | `String "set" -> Set
    | `String "create" -> Create
    | `String "read" -> Read
    | `String "update" -> Update
    | `String "delete" -> Delete
    | _ -> failwith "Irminfs_trans.Kind.of_json"

  let to_json = function
    | Set -> `String "set"
    | Create -> `String "create"
    | Read -> `String "read"
    | Update -> `String "update"
    | Delete -> `String "delete"

  let compare a b = match a, b with
    | Set,    Set    ->  0
    | _,      Set    ->  1
    | Set,    _      -> -1
    | Read,   Read   ->  0
    | _,      Read   ->  1
    | Read,   _      -> -1
    | Create, Create ->  0
    | _,      Create ->  1
    | Create, _      -> -1
    | Update, Update ->  0
    | _,      Update ->  1
    | Update, _      -> -1
    | Delete, Delete ->  0

  let max a b = if compare a b < 0 then b else a
end

module Call = struct
  type t =
    | Pull of int32 * Kind.t Pathm.t
    | Lookup of string list
    | Readdir of string list
    | Symlink of string * string list
    | Rename of string list * string list
    | Unlink of string list
    | Rmdir of string list

  let of_json = function
    | `O ["symlink", `A [`String a; `String b]] -> Symlink (a, split b)
    | `O ["lookup", `String path] -> Lookup (split path)
    | `O ["readdir", `String path] -> Readdir (split path)
    | `O ["unlink", `String path] -> Unlink (split path)
    | `O ["rmdir", `String path] -> Rmdir (split path)
    | `O ["rename", `A [`String src; `String dest]] ->
      Rename (split src, split dest)
    | `O ["pull", `A [`Float remote; `O pathm]] ->
      let paths = List.fold_left (fun m (path, kind) ->
        Pathm.add (Path.of_hum path) (Kind.of_json kind) m
      ) Pathm.empty pathm in
      Pull (Int32.of_float remote, paths)
    | _ -> failwith "Irminfs_trans.Call.of_json"

  let to_json = function
    | Symlink (a,b) ->
      `O ["symlink", `A [`String a; `String (concat b)]]
    | Lookup path ->
      `O ["lookup", `String (concat path)]
    | Readdir path ->
      `O ["readdir", `String (concat path)]
    | Rename (src, dest) ->
      `O ["rename", `A [`String (concat src); `String (concat dest)]]
    | Unlink path ->
      `O ["unlink", `String (concat path)]
    | Rmdir path ->
      `O ["rmdir", `String (concat path)]
    | Pull (pid, paths) ->
      `O ["pull", `A [
        `Float (Int32.to_float pid);
        `O (Pathm.fold (fun p k l ->
          (Path.to_hum p, Kind.to_json k)::l
        ) paths []);
      ]]

  let with_parents children paths =
    let paths = Pathm.of_list paths in
    List.fold_left (fun m (path, k) -> match Path.parent path with
      | None -> m
      | Some parent -> Pathm.add parent k m
    ) paths children

  let paths = Kind.(function
    | Readdir path -> Pathm.of_list [ Path.dir path, Read ]
    | Lookup path -> Pathm.of_list [ Path.ino path, Read ]
    | Symlink (_, path) -> with_parents [ path, Set ] [ Path.ino path, Create ]
    (* TODO: dat to prevent covert channel? *)
    | Unlink path ->
      with_parents [ path, Set ] [ Path.ino path, Delete; Path.dat path, Delete ]
    | Rmdir path ->
      with_parents [ path, Set ] [ Path.ino path, Delete; Path.dir path, Delete ]
    | Rename (path,path') ->
      with_parents [ path, Set; path', Set ] [
        Path.ino path,  Delete; Path.dat path,  Delete;
        Path.ino path', Create; Path.dat path', Create;
      ]
    | Pull (_, paths) -> paths
  )
end

type t = {
  call : Call.t;
  gid  : int32;
  pid  : int32;
  uid  : int32;
}

let of_json = function
  | `O ["call", call;
        "gid", `Float gid;
        "pid", `Float pid;
        "uid", `Float uid;
       ] -> {
      call = Call.of_json call;
      gid = Int32.of_float gid;
      pid = Int32.of_float pid;
      uid = Int32.of_float uid;
    }
  | _ -> failwith "Irminfs_trans.of_json"

let to_json { call; gid; pid; uid } = `O [
  "call", Call.to_json call;
  "gid",  `Float (Int32.to_float gid);
  "pid",  `Float (Int32.to_float pid);
  "uid",  `Float (Int32.to_float uid);
]

let task t = Irmin_unix.task (Ezjsonm.to_string (to_json t))

let paths { call } = Call.paths call

let pull { gid; pid; uid } owner paths =
  { call = Call.Pull (owner, paths); gid; pid; uid }

module Construct(In : In.LINUX_7_8) = struct

  let with_call req call =
    let pid = Ctypes.getf req.Fuse.hdr In.Hdr.pid in
    let uid = Ctypes.getf req.Fuse.hdr In.Hdr.uid in
    let gid = Ctypes.getf req.Fuse.hdr In.Hdr.gid in
    { call; gid; pid; uid }

  let lookup req path = with_call req (Call.Lookup path)
  let readdir req path = with_call req (Call.Readdir path)
  let symlink req target path = with_call req (Call.Symlink (target,path))
  let rename req src dest = with_call req (Call.Rename (src, dest))
  let unlink req path = with_call req (Call.Unlink path)
  let rmdir req path = with_call req (Call.Rmdir path)
end
