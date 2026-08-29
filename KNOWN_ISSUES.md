# Known issues

This is a working list for maintainers, not a user-facing list of product known
issues. It records things found while rebuilding the documentation (see
`DOCS_PLAN.md`, which links here) that turned out to be bugs, dead code, or
places where the docs and the code disagree, so they are not lost once the
docs page that surfaced them is finished. Items are grouped by kind and
checked off as they are fixed. Each item below was verified by reading and, in
most cases, running the code at the point it was added.

## Broken code

- [ ] `tangos.getdb` raises `NameError` when called. It is exported in
      `tangos.query.__all__` (and so is `tangos.getdb`), but the branch that
      handles a `Simulation` being passed where a `Halo` is expected references
      a bare name `Halo` that is never imported into `tangos/query.py` (only
      `HaloProperty, Simulation, SimulationObjectBase, TimeStep` are). Confirmed:
      `'Halo' not in dir(tangos.query)`. It also has zero callers anywhere in
      `tangos/`, `tests/` or `docs/`. Either fix and test it, or remove it from
      `__all__` and delete it.
- [ ] `SimulationObjectBase.plot(name, ...)` always raises `AttributeError`. It
      fetches a `HaloProperty` row and calls `.plot()` on it, but `HaloProperty`
      defines no `plot` method (confirmed: `hasattr(HaloProperty, 'plot')` is
      `False`). Vestigial; either implement a real `plot()` on `HaloProperty` or
      remove the method.
- [ ] `TimeStep.keys()` is a stub: it computes an unused `session` and then
      unconditionally raises `RuntimeError("Not implemented")`. By contrast
      `SimulationObjectBase.keys()` and `Simulation.keys()` both work, so a user
      has no reason to expect this one to be different.
- [ ] `tangos.core.Halo` does not exist (only `tangos.core.halo.Halo`, and
      likewise for `BH`/`Group`/`Tracker`/`PhantomHalo`) — confirmed via
      `hasattr(tangos.core, 'Halo')` is `False`. Relatedly,
      `tangos.core.__all__` omits `TimeStep`, `Simulation`, `HaloProperty` and
      `HaloLink` even though `tangos/core/__init__.py` imports all of them (it
      in fact also omits `SimulationObjectBase`, `TrackData`,
      `get_default_engine`, `set_default_session`, `close_db` and
      `close_session`, which are importable but not re-exported either) — so
      `from tangos.core import *` does not deliver the object model, while
      naming any of these explicitly (`tangos.core.TimeStep`, etc.) works fine.
      Worth fixing `__all__` to match what is actually importable.
- [ ] `tangos.testing.simulation_generator.SimulationGeneratorForTests`:
      `add_objects_to_timestep(10)` assigns particle counts descending from
      1000 in steps of 100, so the 10th halo gets `NDM=0`; a subsequent
      `link_last_halos()` call then divides by zero. Reproduced directly:
      `ZeroDivisionError: float division by zero`. Works fine for `n <= 9`, or
      with an explicit `NDM=` list passed to `add_objects_to_timestep`.
- [ ] `MultiSourceAllMajorProgenitorsStrategy` and
      `MultiSourceAllMajorDescendantsStrategy`, defined in
      `tangos/relation_finding/multi_source.py`, are not re-exported from
      `tangos/relation_finding/__init__.py`, unlike every other strategy in the
      subpackage (confirmed: both are `False` for
      `hasattr(tangos.relation_finding, ...)` but `True` from the
      `multi_source` submodule directly). Looks like an oversight rather than a
      deliberate omission.
- [ ] `tangos.core.init_db` mishandles a bare two-slash `sqlite://` URI (the
      in-memory form). The scheme-detection logic only special-cases the
      three-slash file form (`db_uri.startswith("sqlite:///")`); a two-slash
      URI falls through to the branch intended for server databases
      (`connect_args = {"connect_timeout": timeout}`), which sqlite3 rejects.
      Reproduced: `init_db('sqlite://')` raises
      `TypeError: 'connect_timeout' is an invalid keyword argument for
      Connection()`. A file-based sqlite URI (`init_db('/tmp/x.db')`) is
      unaffected. Found while trying to reproduce the item above.

## Misleading or wrong documentation

- [ ] `docs/custom_input_handlers.md:143` calls `.earlier` on the result of
      `get_halo(...)` (`tangos.get_halo("...").earlier.load()`). `earlier` is
      not an attribute of anything in the codebase; `.previous` is the
      attribute that walks one step back, and `earlier(n)` is a *live-calculation
      function* (a different namespace entirely). Fix when the input-handlers
      how-to is rewritten.
- [ ] `docs/custom_input_handlers.md` presents `match_objects` as part of the
      `HandlerBase` contract, but `HandlerBase` does not define it at all — it
      is defined on `PynbodyInputHandler`, `YtInputHandler`,
      `Gadget4HBTPlusInputHandler` and the test handler only. Either add an
      abstract `match_objects` to `HandlerBase`, or correct the page to
      present it as specific to those subclasses.
- [ ] `docs/first_steps_eagle.md:65` — the second `tangos import-properties`
      command reads `--for RefL0025N037`, missing the trailing `6`
      (`RefL0025N0376`, as used two lines above it).
- [ ] `docs/using_with_yt.md:85` writes a density profile to `tutorial_changa`,
      not `tutorial_changa_yt` (the simulation this tutorial actually adds).
- [ ] `docs/first_steps_ramses+hop.md:88` looks up
      `"tutorial/output_00010/halo_1"`, but the simulation added earlier in the
      same page (line 28) is named `tutorial_ramses`, not `tutorial`.
- [ ] `docs/rdbms.md:34` — "For most users, MySQL and PostgreSQL are" — the
      sentence stops mid-way with no continuation.
- [ ] `README.md:10` claims tangos is "a modular system for Python 3.6+", while
      CI (`.github/workflows/build-test.yaml`) tests 3.11–3.14 only. (Checked
      `docs/index.md` too: it does not repeat this claim anywhere — it has no
      Python-version statement at all — so only `README.md` needs correcting,
      not both files as originally suspected.)
- [ ] `docs/mpi.md` still documents `pypar` as an alternative MPI backend to
      `mpi4py`. pypar is Python-2-era and unmaintained; see the corresponding
      dead-code entry below.
- [ ] Halo-path syntax is inconsistent across the docs — some pages write
      `.../halo_1`, others `.../1` for the same object — and the equivalence is
      explained nowhere. (The curated API reference now explains it once, on
      `docs/reference/api/query.rst`; the tutorial pages themselves still need
      the sweep.)

## Deprecated / vestigial

- [ ] **Replace all remaining `get_halo` references with `get_object`.** The
      project prefers `get_object` everywhere now (`get_halo` is kept as a
      working alias, not marked deprecated, because the tutorials still teach
      it exclusively). Grepping the docs directly:
      `docs/custom_input_handlers.md`, `docs/custom_properties.md`,
      `docs/first_steps_ramses+hop.md`, `docs/histogram_properties.md`,
      `docs/live_calculation.md`, `docs/tracking.md`, `docs/using_with_yt.md`
      (13 occurrences across those seven files), plus 5 occurrences in
      `docs/Data exploration with python.ipynb`. Deliberately **not** swept as
      a mass edit now — every one of these files is being rewritten in stages
      1–5 of `DOCS_PLAN.md`, so `get_object` replaces `get_halo` page by page,
      as each page is rewritten.
- [ ] `config.enable_async_message_processing` should not be advertised, even
      on the forthcoming `configuration` reference page. Its own comment in
      `tangos/config.py` says it "can lead to subtle race conditions", "may
      (?) also be implicated in hangs", that the performance benefit is
      unclear, and that it "may be that async processing should be removed
      from the codebase entirely". Default is `False`; leave it undocumented.
- [ ] The five deprecated console-script shims —
      `tangos_writer`, `tangos_crosslink`, `tangos_timelink`, `tangos_add_bh`,
      `tangos_import_from_ahf` (`setup.py`'s `console_scripts` entry points,
      forwarding to `tangos.scripts.{writer,crosslink,timelink,add_bh,import_from_ahf}.main`)
      — each prints a deprecation notice and forwards to the `tangos`
      subcommand. Candidates for removal once nothing in the wild depends on
      them; in the meantime, do not resurrect them as documented commands in
      `reference/cli`.
- [ ] `tangos/parallel_tasks/backends/pypar.py` is dead: pypar has not been
      maintained since the Python 2 era. `docs/mpi.md` still lists it as an
      alternative to mpi4py (see the docs entry above). Candidate for deletion
      alongside that doc fix.
