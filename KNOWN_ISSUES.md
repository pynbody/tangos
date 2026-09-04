# Known issues

This is a working list for maintainers, not a user-facing list of product known
issues. It records things found while rebuilding the documentation (see
`DOCS_PLAN.md`, which links here) that turned out to be bugs, dead code, or
places where the docs and the code disagree, so they are not lost once the
docs page that surfaced them is finished. Items are grouped by kind and
checked off as they are fixed. Each item below was verified by reading and, in
most cases, running the code at the point it was added.

## Major problems with documentation
- [x] `live_calculations.rst` section on "Live properties" is problematic because
      it leads on the exception rather than the rule. That is `t()`, `z()` etc are
      weird live properties that actually access stored information (albeit in the
      timestep, not the object). And the one "normal" example given doesn't
      actually exist (`Vvir()`). Proposed solution: implement `Vvir()` live property
      within tangos' default shipped live properties. Lead on that, then mention
      `z()`, `t()` etc and note that they are live properties because they're not
      literally stored with the object, even though they are stored. Fixed:
      `Vvir` is now shipped as a live property in `tangos/properties/derived.py`,
      and the documentation section has been rewritten accordingly.

## Broken code

- [ ] `tests/test_live_calculation.py` is order-dependent and fails
      intermittently under `pytest-random-order`, which CI uses
      (`.github/workflows/build-test.yaml:41`). The tests share one database
      built in `setup_module`, and several mutate it, so some orderings leave
      later tests without the rows they expect. Reproduced on a checkout that
      predates the `Vvir` work, with `--random-order-seed=3`: four failures,
      including `test_property_redirection` and `test_nested_abs_at_function`,
      reporting `NoResultsError: Calculation BH.BH_mass returned no results`
      and the equivalent for `abs(at(3.0,dummy_property_2))`. Over ten random
      orderings, five runs failed. This is pre-existing and unrelated to any
      documentation change; it means a red CI run on this file may be
      ordering, not a real regression.

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

- [ ] `config.property_modules` is a one-shot `map` object that is always
      already exhausted by the time anyone can read it. `tangos/config.py:74`
      ends with `property_modules = map(str.strip, property_modules)` rather
      than materialising a list. `tangos/properties/__init__.py:570` consumes it
      when `tangos.properties` is imported, which happens during `import
      tangos`, so afterwards the iterator is spent. Reproduced: after `import
      tangos.config`, `type(...)` is `map` and two successive `list()` calls
      both return `[]`. So the setting cannot be inspected, and any second call
      to `_import_configured_property_modules()` silently imports nothing.
      Setting it to a real list in `config_local.py` is unaffected. Fix:
      `property_modules = [s.strip() for s in property_modules]`.
- [ ] `tangos.get_simulation` treats `_` as a wildcard, so almost every lookup
      by exact name is silently a pattern match. `tangos/query.py:22` switches
      to `Simulation.basename.like(id)` whenever the id contains `%` **or**
      `_`, but in SQL `LIKE` an underscore matches any single character. Every
      tutorial simulation name contains one (`tutorial_changa`), so essentially
      all real lookups go through wildcard matching. Reproduced:
      `tangos.get_simulation('tutorial_gadge_')` — not the name of any
      simulation — returns `<Simulation("tutorial_gadget")>`. In an unlucky
      case this returns the wrong simulation, or raises the "Multiple matches"
      error for a name the user typed exactly. Only `%` should trigger the
      `LIKE` branch.
- [ ] Querying a database that does not exist silently creates an empty one,
      then reports the wrong problem. `tangos/core/__init__.py:170` calls
      `Base.metadata.create_all(_engine)` on every `init_db()` with no check
      for whether the target existed, and for a `sqlite:///` URI that creates
      the file. The user then gets `RuntimeError: No simulation matches ...`
      from `tangos/query.py:29`, which points at their query rather than at
      their `TANGOS_DB_CONNECTION`. Reproduced: with
      `TANGOS_DB_CONNECTION=/tmp/nonexistent_demo.db`, a single
      `get_simulation()` call leaves an 80 KB empty database on disk and
      reports only the "No simulation matches" error. A newcomer with a
      mistyped path hits this immediately, and the empty file they are left
      with makes the next attempt fail the same way.
- [ ] Reading a property's *metadata* requires pynbody, though the metadata is
      entirely in the database. `HaloProperty.description`
      (`tangos/core/halo_data/property.py:80-82`) goes through `self.halo.handler`
      — the handler *instance* — and `Simulation.get_output_handler()`
      (`tangos/core/simulation.py:36-43`) constructs it, which imports pynbody
      at `tangos/input_handlers/pynbody.py:43-44`. Plain data access
      (`halo['Mvir']`) instead uses `handler_class`
      (`tangos/core/halo.py:141-145`), which resolves the class without
      constructing it and so needs no pynbody. The consequence is that
      `halo.get_description(...)` and `HaloProperty.x_values()` raise
      `ModuleNotFoundError: No module named 'pynbody'` on a machine that is
      only ever reading an existing database. Found while checking whether the
      documentation tutorials can be built without pynbody: they cannot, purely
      because of this path.
- [ ] `tangos/config.py:128-131` swallows every error from `config_local.py`,
      not just its absence. The `try: from .config_local import *` is guarded by
      a bare `except: pass`, so a `SyntaxError` or a typo raising `NameError`
      inside a user's `config_local.py` silently discards the whole file. The
      user is left believing their configuration is active when none of it is,
      with no warning. Catching `ImportError` (and letting anything else
      propagate) would be the fix.

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

- [ ] `TimeStep.trackers` also returns black holes, though nothing says so.
      `BH` subclasses `Tracker` (`tangos/core/halo.py:456`) and
      `TimeStep.trackers` (`:493`) is a polymorphic relationship on `Tracker`,
      so `BH` rows come back too. Reproduced on
      `tutorial_changa_blackholes/%960`: `trackers.count()` and `bhs.count()`
      are both 39, and `trackers.first()` is a `BH`.
      `docs/reference/api/objects.rst` lists `trackers` and `bhs` as sibling
      accessors, which reads as though they were disjoint. Either document the
      containment or give `trackers` a filter that excludes black holes.
- [ ] `docs/rdbms.md` gives the wrong install command for the server backends:
      it says `pip install PyMySQL` / `pip install psycopg2-binary` rather than
      `pip install tangos[rmdbs]`, and omits the `[rsa]` extra that
      `setup.py:127` pins because MySQL 8's default `caching_sha2_password`
      auth needs it. A reader following the page as written can install
      PyMySQL and still fail to connect. This is separate from the truncated
      sentence on the same page recorded above. `docs/installation.rst` gives
      the correct form.
- [ ] `tangos/input_handlers/__init__.py:7` sends readers to
      `https://pynbody.github.io/tangos/input_handlers.html`, which does not
      exist and never has — the page is `custom_input_handlers.html`. This is a
      dead link in shipped code, independent of the documentation rebuild.
- [ ] Which `tangos.config` settings can be changed at runtime is inconsistent
      and undocumented. Some are read as `config.X` at the point of use and so
      respond to a later assignment (`config.db`, `config.base`,
      `config.min_halo_particles`, ...), while others are captured by value at
      their consuming module's import time via `from ..config import X` and
      silently ignore any later change — among them every `mergertree_*`
      setting (`tangos/relation_finding/tree.py:9-13`), every `webview_*`
      setting (`tangos/web/routes.py:3`), `DB_IMPORT_CHUNK_SIZE`
      (`tangos/tools/db_importer.py:12`) and
      `DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK` (`tangos/parallel_tasks/lock.py:3`).
      A reader who learns "set `tangos.config.X = ...`" from one example will
      find it silently does nothing for roughly half the settings. The
      `configuration` page must state which is which; better still would be to
      make the access pattern uniform.
- [ ] `TimeStep.trackers` also returns black holes, but
      `docs/reference/api/objects.rst` lists `trackers` and `bhs` as sibling
      accessors, which reads as though they were disjoint sets. `BH`
      subclasses `Tracker` (`tangos/core/halo.py:456`) and `TimeStep.trackers`
      is a polymorphic relationship on `Tracker` (`:493`), so every `BH` row is
      returned by both. Confirmed on `tutorial_changa_blackholes/%960`:
      `trackers.count()` and `bhs.count()` are both 39, and `trackers.first()`
      is a `BH`. Either document the containment or give `trackers` a
      tracker-only variant.

## Deprecated / vestigial

- [ ] Consider changing `.keys()` on halos to return only one instance of each
      key, even where there are multiple links etc. This matches the existing dict-style
      behaviour: the key is passed *once* to `__getitem__`, the multiple values 
      are returned in one shot.
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
