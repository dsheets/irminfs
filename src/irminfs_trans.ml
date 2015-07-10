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

module Call = struct
  type t =
    | Lookup of string list
    | Readdir (* TODO: should contain more info but it's not obvious how *)
    | Symlink of string * string

  let of_json = function
    | `O ["symlink", `A [`String a; `String b]] -> Symlink (a,b)
    | `O ["lookup", `String path] -> Lookup (Stringext.split ~on:'/' path)
    | `String "readdir" -> Readdir
    | _ -> failwith "Irminfs_trans.Call.of_json"
  let to_json = function
    | Symlink (a,b) ->
      `O ["symlink", `A [`String a; `String b]]
    | Lookup path ->
      `O ["lookup", `String (String.concat "/" path)]
    | Readdir -> `String "readdir"
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
let readdir = { call = Call.Readdir }
