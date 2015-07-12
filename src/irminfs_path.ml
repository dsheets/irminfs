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

module Util = Irminfs_util

module Type = struct
  type t = Dir | Ino | Dat

  let compare a b = match a, b with
    | Ino, Ino ->  0
    | Ino, _   -> -1
    | _,   Ino ->  1
    | Dat, Dat ->  0
    | Dat, _   -> -1
    | _,   Dat ->  1
    | Dir, Dir ->  0

  let to_tag = function Dir -> "dir" | Ino -> "ino" | Dat -> "dat"

  let of_tag = function
    | "ino" -> Ino
    | "dir" -> Dir
    | "dat" -> Dat
    | _      -> failwith "Irminfs_node.Path.Type.of_tag"

  let to_ext = function Dir -> ".dir" | Ino -> ".ino" | Dat -> ".dat"
end

type t = {
  typ  : Type.t;
  path : string list;
}

let ino path = { typ=Type.Ino; path }
let dat path = { typ=Type.Dat; path }
let dir path = { typ=Type.Dir; path }

let hd { path } = List.hd path

let get_extension filename =
  let rec search_dot i =
    if i < 1 || filename.[i] = '/' then ""
    else if filename.[i] = '.' then
      String.sub filename (i+1) (String.length filename - i - 1)
    else search_dot (i - 1) in
  search_dot (String.length filename - 1)

module Step = struct
  type t = {
    typ  : Type.t;
    step : string;
  }

  let compare { typ; step } { typ=typ'; step=step' } =
    match Type.compare typ typ' with
    | 0 -> String.compare step step'
    | x -> x

  let equal a b = 0 = compare a b

  let hash = Hashtbl.hash

  let to_json_object { typ; step } = `O [Type.to_tag typ, `String step]

  let to_json : t Tc.to_json = fun t -> Ezjsonm.value (to_json_object t)

  let of_json = function
    | `O [tag, `String step] -> { typ = Type.of_tag tag; step }
    | _                      -> failwith "Irminfs_node.Path.Step.of_json"

  let size_of = Util.size_of_json to_json_object

  let write = Util.write_json to_json_object

  let read = Util.read_json of_json

  let to_hum { typ; step } = step^(Type.to_ext typ)

  let of_hum seg = match get_extension seg with
    | "ino" -> { typ=Type.Ino; step=Filename.chop_suffix seg ".ino" }
    | "dir" -> { typ=Type.Dir; step=Filename.chop_suffix seg ".dir" }
    | "dat" -> { typ=Type.Dat; step=Filename.chop_suffix seg ".dat" }
    | _     -> { typ=Type.Dat; step=seg }
end

type step = Step.t

let compare { typ; path } { typ=typ'; path=path' } =
  match Type.compare typ typ' with
  | 0 -> Irmin.Path.String_list.compare path path'
  | x -> x

let equal a b = 0 = compare a b

let hash = Hashtbl.hash

let empty = { typ=Type.Dir; path=[] }

let create steps =
  let rec aux prev = function
    | []                   -> empty
    | [{ Step.typ; step }] -> { typ; path=List.rev (step::prev) }
    | { Step.step }::rest  -> aux (step::prev) rest
  in
  aux [] steps

let is_empty = function
  | { path=[] }   -> true
  | { path=_::_ } -> false

let cons { Step.step } path = { path with path=step::path.path }

let rcons { path } { Step.typ; step } = { typ; path=List.append path [step] }

let decons = function
  | { path = [] } -> None
  | { typ; path = [s] } -> Some ({ Step.typ; step=s }, empty)
  | { typ; path = step::path } ->
    Some ({ Step.typ=Type.Dir; step }, { typ; path })

let rdecons = function
  | { path = [] } -> None
  | { typ; path } ->
    let rpath = List.rev path in
    Some ({ typ=Type.Dir; path = List.rev (List.tl rpath) },
          { Step.typ; step = List.hd rpath })

let map { typ; path } f =
  let rec aux prev = function
    | []       -> List.rev prev
    | [step]   -> aux ((f { Step.typ; step })::prev)  []
    | step::ss -> aux ((f { Step.typ=Type.Dir; step })::prev) ss
  in
  aux [] path

let to_json_object { typ; path } =
  `O [Type.to_tag typ, Irmin.Path.String_list.to_json path]

let to_json : t Tc.to_json = fun t -> Ezjsonm.value (to_json_object t)

let of_json = Irmin.Path.String_list.(function
  | `O [tag, path] -> { typ = Type.of_tag tag; path = of_json path }
  | _                 -> failwith "Irminfs_node.Path.of_json"
)

let size_of = Util.size_of_json to_json_object

let write = Util.write_json to_json_object

let read = Util.read_json of_json

let to_hum { typ; path } = (String.concat "/" path)^(Type.to_ext typ)

let of_hum path = match get_extension path with
  | "ino" -> {
      typ=Type.Ino;
      path=Stringext.split ~on:'/' (Filename.chop_suffix path ".ino");
    }
  | "dat" -> {
      typ=Type.Dat;
      path=Stringext.split ~on:'/' (Filename.chop_suffix path ".dat");
    }
  | "dir" -> {
      typ=Type.Dir;
      path=Stringext.split ~on:'/' (Filename.chop_suffix path ".dir");
    }
  | _      -> failwith "Irminfs_node.Path.of_hum"
