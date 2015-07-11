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

let split = Stringext.split ~on:'/'
let concat = String.concat "/"

module Call = struct
  type t =
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
end

type t = {
  call : Call.t;
}

let of_json = function
  | `O ["call", call] -> { call = Call.of_json call }
  | _ -> failwith "Irminfs_trans.of_json"

let to_json { call } =
  `O [ "call", Call.to_json call]

let task t = Irmin_unix.task (Ezjsonm.to_string (to_json t))

let lookup path = { call = Call.Lookup path }
let readdir path = { call = Call.Readdir path }
let rename src dest = { call = Call.Rename (src, dest) }
let unlink path = { call = Call.Unlink path }
let rmdir path = { call = Call.Rmdir path }
