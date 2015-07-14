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
module Trans = Irminfs_trans
module Pathm = Trans.Pathm
module Kind  = Trans.Kind
module Space = Irminfs_space

open Lwt.Infix

(* Coherence and causality *)

module Make(Store : Space.COORD) = struct
  module Space = Space.Make(Store)
  module Particles = Set.Make(Int32)
  module Particlem = Map.Make(Int32)
  module Observe = Map.Make(Path) (* path -> pid *)

  type shadow = Trans.t -> Store.t
  type t = Trans.t -> Store.t Lwt.t
  type kind = Kind.t

  module State = struct
    type observation = {
      particles : Particles.t;
      kind : kind;
    }

    type t = {
      position : shadow;
      effects  : kind Pathm.t Particlem.t;
      (** the path perspectives we've affected *)
      observe  : observation Observe.t;
      (** the paths we know are dirty *)
    }

    let empty position = {
      position;
      effects = Particlem.empty;
      observe = Observe.empty;
    }

    let max_effects j k =
      Pathm.merge (fun _path a b -> match a, b with
        | None, None -> None
        | Some x, None | None, Some x -> Some x
        | Some x, Some y -> Some (Kind.max x y)
      ) j k

    let observations paths t =
      let particles = Pathm.fold (fun path _kind map ->
        try
          let { particles; kind } = Observe.find path t.observe in
          Particles.fold (fun particle map ->
            let paths =
              try Particlem.find particle map
              with Not_found -> Pathm.empty
            in
            Particlem.add particle (Pathm.add path kind paths) map
          ) particles map
        with Not_found -> map
      ) paths Particlem.empty in
      Particlem.fold (fun p paths l -> (p,paths)::l) particles []

    let sync t =
      let particles = Observe.fold (fun path observation map ->
        Particles.fold (fun particle map ->
          let paths =
            try Particlem.find particle map
            with Not_found -> Pathm.empty
          in
          Particlem.add particle (Pathm.add path observation.kind paths) map
        ) observation.particles map
      ) t.observe Particlem.empty in
      Particlem.fold (fun p paths l -> (p,paths)::l) particles []

    let actions pid t =
      try Particlem.find pid t.effects
      with Not_found -> Pathm.empty

    let write paths ({ effects } as t) =
      let effects = Particlem.map (max_effects paths) effects in
      { t with effects }

    let interfere waves t =
      let paths = Particlem.fold (fun _ -> max_effects) waves Pathm.empty in
      let observe = Pathm.fold (fun k _ m -> Observe.remove k m) paths t.observe in
      let effects = Particlem.fold (fun p range writes ->
        Particlem.add p begin
          if Particlem.mem p waves
          then Particlem.fold (fun pid set range ->
            if pid = p
            then range
            else max_effects set range
          ) waves range
          else max_effects paths range
        end writes
      ) t.effects t.effects in
      { t with observe; effects }

    (* After a pid merges a state, the old state has no effect on the pid. *)
    let merge pid ({ effects } as t) =
      let effects = Particlem.add pid Pathm.empty effects in
      { t with effects }

    let observe paths pid ({ observe } as t) =
      let observe = Pathm.fold (fun path kind map -> Kind.(
        let obs =
          try Observe.find path map
          with Not_found -> { particles = Particles.empty; kind = Set }
        in
        match kind with
        (* TODO:
             if these fail then we don't have to collapse
             a read will pull sets but an update doesn't have to?
             if we allow superpositions of these, sometimes they will be
               ordered (e.g. for create and delete)
        *)
        | Set when obs.kind = Set ->
          let particles = Particles.add pid obs.particles in
          Observe.add path { obs with particles } map
        | Set | Create | Update | Delete ->
          let observation = {
            particles = Particles.singleton pid;
            kind;
          } in
          Observe.add path observation map
        | Read -> map
      )) paths observe in
      { t with observe }
  end

  module Super = struct
    module System = Map.Make(Int32)
    type t = State.t System.t

    let create position =
      position
      >|= fun position ->
      System.singleton 0_l (State.empty position)

    let find pid system = System.find pid system

    let add pid state system = System.add pid state system

    let effect paths pid system =
      System.mapi (fun p state ->
        if p = pid
        then State.write paths state
        else State.observe paths pid state
      ) system

    let collapse particles pid system =
      let state = find pid system in
      let (system, waves) = List.fold_left (fun (system,map) p ->
        let particle = find p system in
        (System.add p (State.merge pid particle) system,
         Particlem.add p (State.actions pid particle) map)
      ) (system,Particlem.empty) particles in
      let state = State.interfere waves state in
      add pid state system, state
  end

  let create config task =
    Super.create (Store.create config task)
    >|= fun super ->
    let super = ref super in

    let get_state pid trans =
      try Lwt.return (Super.find pid !super)
      with Not_found ->
        let macro = Super.find 0_l !super in
        let store = macro.State.position trans in
        let ic = open_in (Printf.sprintf "/proc/%ld/cmdline" pid) in
        let cmd = input_line ic in
        let nidx = String.index cmd '\000' in
        let cmd = String.sub cmd 0 nidx in
        close_in ic;
        let tag = Printf.sprintf "%ld.%s" pid cmd in
        Store.clone task store tag
        >>= (function
          | `Duplicated_tag
          | `Empty_head  -> Store.of_tag config task tag
          | `Ok position -> Lwt.return position
        )
        >>= fun position ->
        let state = { macro with State.position } in
        super := Super.add pid state !super;
        Lwt.return state
    in

    fun trans ->
      let { Trans.pid } = trans in
      let paths = Trans.paths trans in
      get_state pid trans
      >>= fun state ->
      let forces = match trans with
        | { Trans.call = Trans.Call.Sync } -> State.sync state
        | _ ->
          List.filter (fun (pid, opaths) ->
            Pathm.exists Kind.(fun path -> function
              | Create | Read | Update | Delete -> Pathm.mem path opaths
              | Set ->
                try Pathm.find path opaths <> Set
                with Not_found -> false
            ) paths
          ) (State.observations paths state)
      in
      begin match forces with
        | [] -> Lwt.return state
        | forces ->
          let particles = List.map fst forces in
          let (super', state) = Super.collapse particles pid !super in
          super := super';
          Lwt_list.iter_s (fun (particle,paths) ->
            let pull = Trans.pull trans particle paths in
            get_state particle pull
            >>= fun incoming ->
            State.(Store.merge pull incoming.position ~into:state.position)
            >>= function
            | `Conflict c -> Lwt.fail (Failure ("Pull failure: "^c))
            | `Ok () -> Lwt.return_unit
          ) forces >|= fun () -> state
      end
      >>= fun state ->
      super := Super.effect paths pid !super;
      Lwt.return (state.State.position trans)
end
