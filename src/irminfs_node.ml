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

module Kind = struct
  type t =
    | Symlink of string

  let compare a b = compare a b
  let equal a b = 0 = compare a b

  let hash = Hashtbl.hash

  let of_json = function
    | `O [ "lnk", `String path ] -> Symlink path
    | _ -> failwith "Irminfs_node.Kind.of_json"

  let to_json = function
    | Symlink path -> `O [ "lnk", `String path ]

  let size_of = function
    | Symlink path -> 1 + 2 + (String.length path)

  let write t c =
    let () = match t with
      | Symlink path ->
        let len = String.length path in
        Mstruct.(with_mstruct c (fun m ->
          set_char m '\000';
          set_be_uint16 m len;
          set_string m path;
        ))
    in
    Cstruct.shift c (size_of t)

  let read m = match Mstruct.get_char m with
    | '\000' ->
      let len = Mstruct.get_be_uint16 m in
      Symlink (Mstruct.get_string m len)
    | _ -> failwith "Irminfs_node.Kind.read"
end

module PathSet = Set.Make(Irmin.Path.String_list)
module TcPathSet = Tc.Set(Irmin.Path.String_list)

module Links = struct
  type t = PathSet.t

  let of_json = function
    | `A links -> List.fold_left (fun set -> function
      | `String path -> PathSet.add (Stringext.split ~on:'/' path) set
      | _ -> failwith "Irminfs_node.Links.of_json"
    ) PathSet.empty links
    | _ -> failwith "Irminfs_node.Links.of_json"

  let to_json set = `A
      (PathSet.fold (fun path lst ->
         (`String (String.concat "/" path))::lst
       ) set [])

  let size_of set =
    PathSet.fold (fun path size ->
      size + 2 + (List.fold_left (fun size seg ->
        size + 1 + String.length seg
      ) (-1) path)
    ) set 2

  let write t c =
    let size = PathSet.cardinal t in
    Mstruct.(with_mstruct c (fun m ->
      set_be_uint16 m size;
      PathSet.iter (fun path ->
        let path = String.concat "/" path in
        let len = String.length path  in
        set_be_uint16 m len;
        set_string m path
      ) t
    ));
    Cstruct.shift c (size_of t)

  let read m =
    let size = Mstruct.get_be_uint16 m in
    let rec collect set = function
      | 0 -> set
      | n ->
        let len = Mstruct.get_be_uint16 m in
        let str = Mstruct.get_string m len in
        PathSet.add (Stringext.split ~on:'/' str) set
    in
    collect PathSet.empty size
end

let merge_kind _path = Irmin.Merge.default (module Tc.Option(Kind))

let merge_monotonic s0 ~old t1 t2 =
  Lwt.return (`Ok (match Tc.compare s0 t1 t2 with
    | 0 -> t1
    | x when x < 0 -> t2
    | _ -> t1
  ))

let merge_bitdiff : int Irmin.Merge.t = fun ~old t1 t2 ->
  Irmin.Merge.promise_map (fun old ->
    let t1d = old lxor t1 in
    let t2d = old lxor t2 in
    let d   = t1d lor t2d in
    old lxor d
  ) old ()
  >|= function
  | `Ok (Some r) -> `Ok r
  | `Ok None -> `Conflict "Irminfs_node.merge_bitdiff"
  | `Conflict s -> `Conflict s

let merge_mode = Irmin.Merge.option Tc.int merge_bitdiff

module type CONSTRUCTOR = sig
  type 'a t
end

module Type(Cons : CONSTRUCTOR) = struct
  type t = {
    ino : int64 Cons.t;
    atime : int64 Cons.t;
    mtime : int64 Cons.t;
    ctime : int64 Cons.t;
    kind : Kind.t Cons.t;
    mode : int Cons.t;
    links : Links.t Cons.t;
    uid : int32 Cons.t;
    gid : int32 Cons.t;
  }
end

module Id = struct type 'a t = 'a end

module Result = struct type 'a t = 'a option Irmin.Merge.result Lwt.t end

module T = Type(Id)
module R = Type(Result)
type t = T.t

let check_merge r result = result >|= function
| `Conflict s -> r := Some (`Conflict s)
| `Ok None -> r := Some (`Conflict "Irminfs_node.check_merge")
| `Ok (Some v) -> r:= Some (`Ok v)

let join_merge { R.ino; atime; mtime; ctime; kind; mode; links; uid; gid } =
  let ino_r   = ref None in
  let atime_r = ref None in
  let mtime_r = ref None in
  let ctime_r = ref None in
  let kind_r  = ref None in
  let mode_r  = ref None in
  let links_r = ref None in
  let uid_r   = ref None in
  let gid_r   = ref None in
  Lwt.join [
    check_merge ino_r ino;
    check_merge atime_r atime;
    check_merge mtime_r mtime;
    check_merge ctime_r ctime;
    check_merge kind_r kind;
    check_merge mode_r mode;
    check_merge links_r links;
    check_merge uid_r uid;
    check_merge gid_r gid;
  ] >|= fun () ->
  match !ino_r,
        !atime_r, !mtime_r, !ctime_r,
        !kind_r, !mode_r, !links_r,
        !uid_r, !gid_r with
  | Some (`Ok ino),
    Some (`Ok atime),
    Some (`Ok mtime),
    Some (`Ok ctime),
    Some (`Ok kind),
    Some (`Ok mode),
    Some (`Ok links),
    Some (`Ok uid),
    Some (`Ok gid) ->
    `Ok { T.ino; atime; mtime; ctime; kind; mode; links; uid; gid }
  | Some (`Conflict s), _, _, _, _, _, _, _, _
  | _, Some (`Conflict s), _, _, _, _, _, _, _
  | _, _, Some (`Conflict s), _, _, _, _, _, _
  | _, _, _, Some (`Conflict s), _, _, _, _, _
  | _, _, _, _, Some (`Conflict s), _, _, _, _
  | _, _, _, _, _, Some (`Conflict s), _, _, _
  | _, _, _, _, _, _, Some (`Conflict s), _, _
  | _, _, _, _, _, _, _, Some (`Conflict s), _
  | _, _, _, _, _, _, _, _, Some (`Conflict s) -> `Conflict s
  | _ -> failwith "Irminfs_node.join_merge"

open T

let opt_kind = function
  | Some { kind } -> Some kind
  | None -> None
let opt_ino = function
  | Some { ino } -> Some ino
  | None -> None
let opt_atime = function
  | Some { atime } -> Some atime
  | None -> None
let opt_ctime = function
  | Some { ctime } -> Some ctime
  | None -> None
let opt_mtime = function
  | Some { mtime } -> Some mtime
  | None -> None
let opt_mode = function
  | Some { mode } -> Some mode
  | None -> None
let opt_links = function
  | Some { links } -> Some links
  | None -> None
let opt_uid = function
  | Some { uid } -> Some uid
  | None -> None
let opt_gid = function
  | Some { gid } -> Some gid
  | None -> None

let compare a b = compare a b
let equal a b = 0 = compare a b

let hash = Hashtbl.hash

let sort_json_object = function
  | `O fields -> `O (List.sort (fun (k, _) (k',_) -> String.compare k k') fields)
  | json -> json

let of_json : T.t Tc.of_json = fun json -> match sort_json_object json with
  | `O [ "atime", `Float atime;
         "ctime", `Float ctime;
         "gid",   `Float gid;
         "ino",   `Float ino;  (* TODO: This could be the full range of int64 *)
         "kind",  kind;
         "links", links;
         "mode",  `Float mode;
         "mtime", `Float mtime;
         "uid",   `Float uid;
       ] ->
    let atime = Int64.of_float atime in
    let ctime = Int64.of_float ctime in
    let mtime = Int64.of_float mtime in
    let ino   = Int64.of_float ino in
    let mode  = int_of_float mode in
    let uid   = Int32.of_float uid in
    let gid   = Int32.of_float gid in
    {
      atime; ctime; mtime; ino;
      kind = Kind.of_json kind;
      mode; uid; gid;
      links = Links.of_json links;
    }
  | _ -> failwith "Irminfs_node.of_json"

let to_json : T.t Tc.to_json = function
  | { atime; ctime; ino; kind; mtime; mode; links; uid; gid } ->
    `O [ "atime", `Float (Int64.to_float atime);
         "ctime", `Float (Int64.to_float ctime);
         "uid",   `Float (Int32.to_float uid);
         "ino",   `Float (Int64.to_float ino); (* TODO: FIXME *)
         "kind",  Kind.to_json kind;
         "links", Links.to_json links;
         "mode",  `Float (float_of_int mode);
         "mtime", `Float (Int64.to_float mtime);
         "gid",   `Float (Int32.to_float gid);
       ]

let size = function
  | { T.kind = Kind.Symlink path } -> Int64.of_int (String.length path)

let size_of t =
  8 + (* ino *)
  8 + 8 + 8 + (* time * 3 *)
  Kind.size_of t.kind +
  2 + (* mode *)
  Links.size_of t.links +
  4 + 4 (* uid, gid *)

let write t c =
  let c = Kind.write t.kind c in
  Mstruct.(with_mstruct c (fun m ->
    set_be_uint64 m t.ino;
    set_be_uint64 m t.atime;
    set_be_uint64 m t.ctime;
    set_be_uint64 m t.mtime;
    set_be_uint16 m t.mode;
    set_be_uint32 m t.uid;
    set_be_uint32 m t.gid;
  ));
  let c = Cstruct.shift c (8 + 8 + 8 + 8 + 2 + 4 + 4) in
  Links.write t.links c

let read m =
  let kind  = Kind.read m in
  let ino   = Mstruct.get_be_uint64 m in
  let atime = Mstruct.get_be_uint64 m in
  let ctime = Mstruct.get_be_uint64 m in
  let mtime = Mstruct.get_be_uint64 m in
  let mode  = Mstruct.get_be_uint16 m in
  let uid   = Mstruct.get_be_uint32 m in
  let gid   = Mstruct.get_be_uint32 m in
  let links = Links.read m in
  { kind; ino; atime; ctime; mtime; mode; links; uid; gid }

module Path = Irmin.Path.String_list

let merge path : t option Irmin.Merge.t = fun ~old t1 t2 ->
  match t1, t2 with
  | Some t1, Some t2 ->
    let kind = merge_kind path
        ~old:(Irmin.Merge.promise_map opt_kind old)
        (Some t1.kind) (Some t2.kind)
    in
    let ino = Irmin.Merge.default (module Tc.Option(Tc.Int64))
      ~old:(Irmin.Merge.promise_map opt_ino old)
      (Some t1.ino) (Some t2.ino)
    in
    let atime = merge_monotonic (module Tc.Option(Tc.Int64))
      ~old:(Irmin.Merge.promise_map opt_atime old)
      (Some t1.atime) (Some t2.atime)
    in
    let mtime = merge_monotonic (module Tc.Option(Tc.Int64))
      ~old:(Irmin.Merge.promise_map opt_mtime old)
      (Some t1.mtime) (Some t2.mtime)
    in
    let ctime = merge_monotonic (module Tc.Option(Tc.Int64))
      ~old:(Irmin.Merge.promise_map opt_ctime old)
      (Some t1.ctime) (Some t2.ctime)
    in
    let mode = merge_mode
        ~old:(Irmin.Merge.promise_map opt_mode old)
        (Some t1.mode) (Some t2.mode)
    in
    let links = Irmin.Merge.(
      option (module TcPathSet)
        (set (module PathSet : Set.S with type t = PathSet.t))
    ) ~old:(Irmin.Merge.promise_map opt_links old)
        (Some t1.links) (Some t2.links)
    in
    let uid = Irmin.Merge.default (module Tc.Option(Tc.Int32))
      ~old:(Irmin.Merge.promise_map opt_uid old)
      (Some t1.uid) (Some t2.uid)
    in
    let gid = Irmin.Merge.default (module Tc.Option(Tc.Int32))
      ~old:(Irmin.Merge.promise_map opt_gid old)
      (Some t1.gid) (Some t2.gid)
    in
    join_merge { R.kind; ino; atime; ctime; mtime; mode; links; uid; gid }
    >|= (function
      | `Conflict s -> `Conflict s
      | `Ok t -> `Ok (Some t)
    )
  | Some t1, None -> Lwt.return (`Ok (Some t1))
  | None, Some t2 -> Lwt.return (`Ok (Some t2))
  | None, None -> Lwt.return (`Ok None)

let ino = ref 1

let next_ino () = let i = !ino in incr ino; i
