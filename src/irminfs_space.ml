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

module Path = Irminfs_path
module Bundle = Irminfs_bundle

module type COORD = sig
  type key = Path.t
  type value = Cstruct.t
  include Irmin.BASIC with type key := key and type value := value
end

module Ino = Irminfs_ino

let ino : Ino.Tc.t Tc.t = (module Ino.Tc)

module Make(Store : COORD) = struct

  type _ typ =
    | Ino : Ino.t typ
    | Dat : Cstruct.t typ

  type 'a t = 'a typ

  let cast_path (type a) (typ : a typ) path = match typ with
    | Ino -> Path.ino path
    | Dat -> Path.dat path

  module View = struct
    include Irmin.View(Store)

    let mem_any = mem

    let mem typ view path = match typ with
      | Ino -> mem view (Path.ino path)

    let update (type a) (typ : a typ) view path (value : a) = match typ with
      | Ino ->
        update view (Path.ino path) (Tc.write_cstruct ino value)
      | Dat ->
        update view (Path.dat path) value
  end

  let read typ store path = match typ with
    | Ino ->
      Store.read store (Path.ino path)
      >|= function
      | None -> None
      | Some cstruct -> Some (Tc.read_cstruct ino cstruct)

  let create store path bundle =
    View.of_path store Path.empty
    >>= fun root ->
    View.mem Ino root path
    >>= function
    | true -> Lwt.return_false
    | false ->
      View.update Ino root path bundle.Bundle.ino
      >>= fun () -> begin
        match bundle.Bundle.dat with
        | None -> Lwt.return_unit
        | Some dat ->
          View.update Dat root path dat
          >>= fun () -> Lwt.return_unit
      end >>= fun () ->
      View.update_path store Path.empty root
      >>= fun () -> Lwt.return_true

  let delete store path =
    View.of_path store Path.empty
    >>= fun root ->
    View.mem Ino root path
    >>= function
    | false -> Lwt.return_false
    | true ->
      View.remove root (Path.ino path)
      >>= fun () ->
      View.remove root (Path.dat path)
      >>= fun () ->
      View.update_path store Path.empty root
      >>= fun () ->
      Lwt.return_true

  let size store path = Ino.(function
    | { T.kind = Kind.Symlink path } -> Int64.of_int (String.length path)
  )

  (* Lists inodes and data nodes but not tree nodes *)
  let list_nodes store path =
    View.of_path store (Path.dir path)
    >>= fun view ->
    View.list view Path.empty
    >>= Lwt_list.filter_p (View.mem_any view)

  let bundle view path =
    View.read view (Path.ino path)
    >>= function
    | None -> Lwt.return_none
    | Some cstruct ->
      let ino = Tc.read_cstruct ino cstruct in
      View.read view (Path.dat path)
      >|= fun dat ->
      Some { Bundle.ino; dat }
end
