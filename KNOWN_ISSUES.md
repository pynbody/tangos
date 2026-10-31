# Known issues

This is a working list for maintainers, not a user-facing list of product known
issues. It records things found while rebuilding the documentation (see
`DOCS_PLAN.md`, which links here) that turned out to be bugs, dead code, or
places where the docs and the code disagree, so they are not lost once the
docs page that surfaced them is finished. Items are grouped by kind and
checked off as they are fixed. Each item below was verified by reading and, in
most cases, running the code at the point it was added.

Every open item names one owner, assigned in the stage 4 triage: **stage 4** or
**stage 5** for a documentation error on a page that stage rewrites, which it
fixes as part of the rewrite; **stage 6** for a code fix, made as its own small
PR against `master` (see **Stage 6** in `DOCS_PLAN.md`). Where a page depends
on a code fix, the item says which page and what it should do meanwhile. Items
closed without a fix say why.

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
      **Owner: stage 6.**

- [ ] `tangos.getdb` raises `NameError` when called. It is exported in
      `tangos.query.__all__` (and so is `tangos.getdb`), but the branch that
      handles a `Simulation` being passed where a `Halo` is expected references
      a bare name `Halo` that is never imported into `tangos/query.py` (only
      `HaloProperty, Simulation, SimulationObjectBase, TimeStep` are). Confirmed:
      `'Halo' not in dir(tangos.query)`. It also has zero callers anywhere in
      `tangos/`, `tests/` or `docs/`. Either fix and test it, or remove it from
      `__all__` and delete it.
      **Owner: stage 6.** Stage 5's API reference depends on it: document the fixed
      function, or leave it out if the fix removes it from `__all__`.
- [ ] `SimulationObjectBase.plot(name, ...)` always raises `AttributeError`. It
      fetches a `HaloProperty` row and calls `.plot()` on it, but `HaloProperty`
      defines no `plot` method (confirmed: `hasattr(HaloProperty, 'plot')` is
      `False`). Vestigial; either implement a real `plot()` on `HaloProperty` or
      remove the method.
      **Owner: stage 6.**
- [ ] `TimeStep.keys()` is a stub: it computes an unused `session` and then
      unconditionally raises `RuntimeError("Not implemented")`. By contrast
      `SimulationObjectBase.keys()` and `Simulation.keys()` both work, so a user
      has no reason to expect this one to be different.
      **Owner: stage 6.**
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
      **Owner: stage 6.** Stage 5's API reference depends on it: it should document
      what `tangos.core` actually exports once this is fixed.
- [ ] `tangos.testing.simulation_generator.SimulationGeneratorForTests`:
      `add_objects_to_timestep(10)` assigns particle counts descending from
      1000 in steps of 100, so the 10th halo gets `NDM=0`; a subsequent
      `link_last_halos()` call then divides by zero. Reproduced directly:
      `ZeroDivisionError: float division by zero`. Works fine for `n <= 9`, or
      with an explicit `NDM=` list passed to `add_objects_to_timestep`.
      **Owner: stage 6.** Stage 5's executable fallback for the language reference
      depends on it if it needs more than nine halos per timestep.
- [ ] `MultiSourceAllMajorProgenitorsStrategy` and
      `MultiSourceAllMajorDescendantsStrategy`, defined in
      `tangos/relation_finding/multi_source.py`, are not re-exported from
      `tangos/relation_finding/__init__.py`, unlike every other strategy in the
      subpackage (confirmed: both are `False` for
      `hasattr(tangos.relation_finding, ...)` but `True` from the
      `multi_source` submodule directly). Looks like an oversight rather than a
      deliberate omission.
      **Owner: stage 6.**
- [ ] `tangos.core.init_db` mishandles a bare two-slash `sqlite://` URI (the
      in-memory form). The scheme-detection logic only special-cases the
      three-slash file form (`db_uri.startswith("sqlite:///")`); a two-slash
      URI falls through to the branch intended for server databases
      (`connect_args = {"connect_timeout": timeout}`), which sqlite3 rejects.
      Reproduced: `init_db('sqlite://')` raises
      `TypeError: 'connect_timeout' is an invalid keyword argument for
      Connection()`. A file-based sqlite URI (`init_db('/tmp/x.db')`) is
      unaffected. Found while trying to reproduce the item above.
      **Owner: stage 6.**

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
      **Owner: stage 6.**
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
      **Owner: stage 6.** Stage 5's API reference and FAQ depend on it: document the
      fixed lookup, or warn about the wildcard if it is still unfixed.
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
      **Owner: stage 6.** Stage 5's FAQ depends on it: describe the fixed error, or
      say how to recognise this one if it is still unfixed.
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
      **Owner: stage 6.** Once fixed, pynbody can leave the `docs` extra (see
      **Decisions already made** in `DOCS_PLAN.md`).
- [ ] `tangos/config.py:128-131` swallows every error from `config_local.py`,
      not just its absence. The `try: from .config_local import *` is guarded by
      a bare `except: pass`, so a `SyntaxError` or a typo raising `NameError`
      inside a user's `config_local.py` silently discards the whole file. The
      user is left believing their configuration is active when none of it is,
      with no warning. Catching `ImportError` (and letting anything else
      propagate) would be the fix.
      **Owner: stage 6.**

- [ ] `tangos/scripts/import_from_ahf.py:16` tells the user to run a command
      that does not exist. The deprecation notice says the `tangos_import_from_ahf`
      script is "deprecated in favour of `tangos property-import`", but the
      subcommand is `import-properties` (`tangos/tools/property_importer.py:12`
      sets `tool_name = 'import-properties'`). So the one message a user of the
      deprecated script sees points them at a name that fails. The same wrong
      name was in `docs/reference/api/building.rst:112` and is fixed there in
      stage 2 of the documentation rebuild.
      **Owner: stage 6.**
- [ ] `tangos/input_handlers/ahf_trees.py:38,42` uses bare `except:` around both
      the standard and the cropped mtree read, so any genuine failure -- a
      malformed file, a `MemoryError`, a bug in the reader -- is reported to the
      user as `OSError("Could not load merger tree files")`, i.e. as a missing
      file. Catching the specific read errors would leave real faults visible.
      **Owner: stage 6.**
- [ ] `AHFTreeImporter.filename_to_snapnum`
      (`tangos/tools/ahf_merger_tree_importer.py:31-37`) is dead code:
      `run_calculation_loop` never calls it, and it hard-codes a `snapdir_###`
      layout that would not match the ChaNGa data the tool is actually tested
      against (`tests/test_ahf_trees.py`).
      **Owner: stage 6.**
- [ ] `tangos import-subfind-trees` is the only merger-tree importer with no
      test, no `build.sh` coverage and no documentation page. It is the intended
      route for Gadget-4/Arepo's on-the-fly trees
      (`tangos/tools/subfind_merger_tree_importer.py`), and
      `build_gadget4()` in `test_tutorial_build/build.sh` builds no merger tree
      at all, so the whole Gadget-4 tree path is unverified. The other two
      importers (`import-consistent-trees`, `import-ahf-trees`) are both tested.
      `docs/reference/simulation_formats` documents it, and says it is untested.
      **Owner: stage 6.** Probably the largest stage 6 item after the 32-bit
      integers: a test needs a fixture with SubFind's own merger trees.
- [ ] `build_enzo_yt` in `test_tutorial_build/build.sh` is wrapped in
      `if [ -d enzo.tinycosmo ]`, so if the download does not produce the
      directory the entire Enzo/yt combination silently drops out of the built
      database -- and hence out of the `tangos diff` comparison that the
      integration test relies on -- rather than failing loudly.
      **Owner: stage 6.**
- [ ] `HandlerBase.load_object` names its third parameter `catalog_pos`, while
      every implementation and every caller uses `finder_offset`. The API
      reference therefore documents a parameter name that does not exist
      downstream.
      **Owner: stage 6.** Stage 4's `how-to/custom_handlers` depends on it: it
      documents `finder_offset`, the name every implementation uses, and notes the
      base-class mismatch until this is fixed.
- [ ] `match_reduce` crashes if any linked object lacks the property being
      reduced, rather than skipping it. `calculation.values()` yields `None`
      for such an object and the reduction is applied to the raw list, so
      `'sum'` and `'mean'` raise `TypeError: unsupported operand type(s) for
      +: 'NoneType' and 'NoneType'` (and `'min'`/`'max'` would compare `None`
      with numbers). Reproduced against the tutorial database:
      `tangos.get_timestep("tutorial_changa/%960").calculate_all(lambda:
      match_reduce('tutorial_changa/%832', star_mass_profile[-1], 'sum'))`;
      the same call with `Mvir`, which every object has, works.
      `tests/test_live_calculation_reduce.py` only covers targets that all
      carry the property. Found in stage 3, where the docs example had to
      switch to `Mvir` to run.
      **Owner: stage 6.**
- [x] Time-chunked histograms (`SFR_histogram`, `BH_mdot_histogram`) have gaps
      wherever snapshots are more than 0.5 Gyr apart. **Fixed in stage 3** (on the
      docs branch, at AP's request, so the docs could show sensible histories):
      `store_slice` now reaches back to the previous timestep, never less than
      `minimum_store_Gyr`, and to the beginning at the first timestep. The
      previous timestep is looked up once per timestep in
      `TimeChunkedProperty.preloop`, not during the calculation (an earlier
      version queried from `store_slice`); tested in
      `tests/test_histogram_property.py`, including that `store_slice` makes no
      query. Two follow-ups in the same change, in
      `BHAccHistogram` (`tangos/properties/pynbody/BH.py`), tested in
      `tests/test_bh_histogram.py`: a chunk no longer stores the leading run of
      NaNs before the black hole's log begins, which the whole-history first
      chunk made long; and the accretion rate is sampled at each bin's start
      time, where `np.linspace(0, 20, nbins)` had a spacing of 20/(nbins-1) and
      drifted off the histogram's bins. The tutorial and CI reference databases
      still hold old-style chunks until rebuilt (see **Before merging to master**
      in `DOCS_PLAN.md`).
      `TimeChunkedProperty.store_slice` (`tangos/properties/__init__.py`)
      always returns the fixed window `[t - minimum_store_Gyr, t]`, and the SF
      and BH properties store exactly that slice. So a snapshot stores the 0.5
      Gyr before it, whatever the spacing to the previous snapshot, and the
      history between them is never stored. The attribute's name says
      *minimum*, which suggests the intent was the larger of 0.5 Gyr and the
      gap to the previous snapshot. Visible in the tutorial database, whose
      ChaNGa snapshots are about 0.65 Gyr apart: `halo_1`'s reassembled SFR
      drops to zero just after 1.53 and 2.17 Gyr, and is zero before 0.81 Gyr
      (0.5 Gyr before the first snapshot). `tutorials/time_series` explains the
      gaps to the reader rather than hiding them. Fixing it means storing the
      interval back to the previous timestep, then rebuilding the tutorial
      database and `reference_database.db`. Found in stage 3.
- [ ] Multi-hop strategies can fail intermittently with `sqlalchemy.exc.InvalidRequestError:
      Table 'multihoplink_final_xxxx' is already defined for this MetaData instance`.
      Seen once in CI on a stage 3 commit (`test_hop_strategy.py::test_multisource_forwards`,
      MySQL, Python 3.14), in code the commit did not touch; the same job passed
      on neighbouring commits. By reading `tangos/relation_finding/multi_hop.py`:
      the temporary tables are named with just four random lowercase letters
      (`rstr`, 26^4 ≈ 457k names), and a table can stay registered in
      `core.Base.metadata` after use. `_manage_temp_table` is a `@contextmanager`
      without `try/finally`, so an exception inside the block skips
      `_delete_temp_table`; and `_temp_table_without_leaving_sql` only removes
      its table through a callback that runs if the caller exits the returned
      halolist context. Once a table has leaked, any later draw of the same four
      letters collides. Proposed fix: wrap the `yield` in `try/finally`, and use a
      longer suffix (e.g. `uuid.uuid4().hex[:16]`) for the table and index names.
      **Owner: stage 6.** Still present on master after #309, which fixed the
      ordering of these temporary tables but not their naming or cleanup.
- [ ] `--backend` handling in `tangos.parallel_tasks._process_command_line()`
      breaks any argument that contains a space. On import it joins `sys.argv`
      with spaces, cuts `--backend <name>` out with a regex, and assigns
      `sys.argv = new_command_line.split()`, discarding the shell's quoting.
      So `tangos write ... --include-only="NDM() > 1000" --backend mpi4py`
      reaches argparse as `NDM()`, `>`, `1000`, while the same command without
      `--backend` works. The regex would also catch `--backend` inside a quoted
      argument. Every documented command happens to avoid spaces inside
      arguments, which is why the docs have not hit this. Relatedly,
      `tangos write` declares its own `--backend` option
      (`tangos/tools/property_writer.py`), which is dead: the import-time
      stripping always removes `--backend` before argparse runs. Found while
      writing `tests/test_docs_commands.py` in stage 3.
      **Owner: stage 6.** Stage 4's `how-to/parallel` depends on it: it warns about
      quoted arguments containing spaces until this is fixed.

- [ ] `master` tracks eight images under `docs/plots/`
      (`quickstart_uvi_image.png`, `time_series_mass_growth.png` and six
      more), committed by accident in
      #310. `docs-refactor-main` gitignores that directory, because it is the
      `@savefig` working directory that every docs build rewrites; but
      `.gitignore` does not apply to files already tracked, so the next merge of
      `master` into `docs-refactor-main` brings them in, and from then on every
      docs build leaves the working tree dirty. Fix: `git rm --cached` them on
      `master`. Found in the stage 4 triage.
      **Owner: stage 6.**
- [ ] `tangos write` fails for every object if the handler gives a timestep no
      redshift. The writer always evaluates the intrinsic live properties, and
      `tangos/properties/intrinsic.py:10` computes `1./(1.+ts.redshift)` along
      with them, so a `None` redshift raises `TypeError: unsupported operand
      type(s) for +: 'float' and 'NoneType'`. `HandlerBase.get_timestep_properties`
      does not say a redshift is required; the old `custom_input_handlers.md`
      example returned only `time_gyr` and so could never have reached `tangos
      write`. Found writing `how-to/custom_handlers` in stage 4a, which now tells
      handler authors to supply both.
      **Owner: stage 6.**
- [ ] `HandlerBase.enumerate_objects`' docstring says it yields
      "halo_number, NDM, NStar, Ngas", but `SimulationAdderUpdater` unpacks five
      values per object -- finder offset, finder id, NDM, NStar, NGas
      (`tangos/tools/add_simulation.py:130`) -- and every handler yields those
      five (or a bare number, which is padded). The docstring appears in the
      API reference (`docs/reference/api/input_handlers.rst`).
      Found in stage 4a.
      **Owner: stage 6.**
- [ ] `tangos flag-duplicates` always fails: its second statement runs
      `update halolink set deprecated=0`, but `HaloLink` has no `deprecated`
      column (`sqlalchemy.exc.OperationalError: no such column: deprecated`,
      reproduced on sqlite). Its sibling `remove-duplicates` works. Found in
      stage 4a; `how-to/managing_a_database` documents only `remove-duplicates`.
      **Owner: stage 6.**
- [ ] After `tangos write --force`, a property has two rows per object, and the
      ways of reading it disagree: `halo["name"]` returns both values as a
      list, while `halo.calculate("name")` and `calculate_all` return one.
      Reproduced with the `how-to/custom_handlers` example: `[42.0, 42.0]`
      against `42.0`. `tangos remove-duplicates` restores a single value. Either
      `--force` should replace the old row, or `__getitem__` should return the
      latest as the live calculation does. `how-to/managing_a_database` tells
      readers to run `remove-duplicates` after `--force` until then. Found in
      stage 4a.
      **Owner: stage 6.**
- [ ] `tangos rollback` of a run that created objects leaves orphans behind if
      later runs added properties or links to them: rolling back the `tangos
      add` of `tutorial_ramses` alone, on a copy of the tutorial database, left
      20323 properties and 24360 links pointing at halos that no longer exist.
      Rolling back every run for the simulation, newest first, leaves none.
      There is also no command to remove a simulation (`tangos rm` exists in
      `tangos/scripts/manager.py` but is commented out as not working), and no
      test of `rollback` or `grep-remove` at all. `how-to/managing_a_database`
      documents the newest-first procedure. Found in stage 4a.
      **Owner: stage 6.**
- [ ] `tangos thin-timesteps` describes its argument as "the maximum interval
      between timesteps", but it is the *minimum* spacing it keeps: it removes
      every timestep that comes less than that interval after the last one it
      kept. On `tutorial_gadget`, `thin-timesteps 2.5` keeps five of eleven
      timesteps, every gap between them at least 2.5 Gyr. Found in stage 4a.
      **Owner: stage 6.**
- [ ] `tangos write --part N M` ("emulate MPI by handling slice N out of the
      total workload of M items") is parsed and never used: nothing reads
      `options.part`. Either implement it or remove the option. Found in stage
      4a; not documented.
      **Owner: stage 6.**
- [ ] `config.PROPERTY_WRITER_MINIMUM_TIME_BETWEEN_COMMITS` has no effect.
      `PropertyWriter.__init__` copies it to `self._writer_minimum`, which
      nothing reads; `_is_commit_needed` commits at the end of every timestep
      regardless. `docs/configuration.rst` said otherwise until stage 4a, and
      now says it has no effect. Found in stage 4a.
      **Owner: stage 6.**
- [ ] `tangos write --include-only` always logs "User-specified inclusion
      criterion excluded 0 of N objects": the message
      (`tangos/tools/property_writer.py:257`) subtracts the number of objects
      *before* the filter is applied on the next line. Found in stage 4a.
      **Owner: stage 6.**
- [ ] `tangos import` into PostgreSQL leaves the database unwritable. It copies
      rows with their ids (offset past the target's existing maximum) but never
      advances PostgreSQL's id sequences, so the next command that inserts a
      row -- the first thing any `tangos write` does is create its run's
      `creators` row -- fails with `duplicate key value violates unique
      constraint "creators_pkey"`. Reproduced on PostgreSQL 16 by importing a
      sqlite database into an empty server database and then running `tangos
      write`: `creators_id_seq` stood at 2 against a maximum id of 5. MySQL and
      sqlite assign new ids from the table's maximum, so are unaffected.
      `tests/test_import.py` runs against PostgreSQL in CI but never writes
      after importing. Fix: after copying, `setval` each sequence to its
      column's maximum on PostgreSQL. Found in stage 4a.
      **Owner: stage 6.** `how-to/database_backends` depends on it: until it is
      fixed, it gives a SQL script that resets the sequences after an import,
      and should drop that warning once it is.
- [ ] `tangos import-changa-bh` never links black holes through time, so no
      black hole in the tutorial database, or in the CI reference database, has a
      progenitor: `BH_mass` cannot be followed back, and a reassembled
      `BH_mdot_histogram` covers only the last 0.5 Gyr. Two faults, one behind the
      other. `_timelink_black_holes` (`tangos/tools/changa_bh_importer.py`) globs
      for `*.BHmergers`, but the tutorial simulations' log is `.mergers` (as
      `docs/reference/simulation_formats.rst` and the old black holes page both
      say), so it logs `No merger file for tutorial_changa_blackholes` and
      returns. With a symlink under the expected name (tried on the CI data),
      linking then fails with `AttributeError: module 'numpy' has no attribute
      'in1d'`: `np.in1d` is gone from current numpy (2.5.3 here), and is also used
      by `tangos.tracking.generate_tracker_halo_links` and by the yt handler's
      `match_objects` (`tangos/input_handlers/yt.py:103`), so linking with yt is
      likely broken the same way. CI tests numpy 2.0 and 2.3.3 only, while
      `setup.py` requires just `numpy >= 2.0`. Found in stage 4b.
      **Owner: stage 6.** `tutorials/black_holes` depends on it: it warns that
      black holes are not linked through time; once fixed, rebuild the tutorial
      and CI reference databases and drop the warning.
- [ ] Redefining a property class in a running session has no effect: tangos
      goes on using the first class registered for a name. `providing_class` and
      `all_providing_classes` (`tangos/properties/__init__.py`) are
      `functools.lru_cache`d, and even after clearing those caches the earlier
      class, still in `PropertyCalculation._all_classes`, is chosen. The
      metaclass comment says reloaded classes overwrite their old versions; they
      do not. So "edit your module and reload it", which the old custom
      properties page advised, silently keeps the old code until python
      restarts. `tutorials/custom_properties` now says to restart. Found in
      stage 4b.
      **Owner: stage 6.**
- [ ] A property module that cannot be imported is skipped silently.
      `_import_configured_property_modules` reports the failure as an
      `ImportWarning`, which python hides by default, so a misspelled
      `TANGOS_PROPERTY_MODULES` gives no message at all until a property is
      reported unknown. Reproduced with `TANGOS_PROPERTY_MODULES=typo`.
      `tutorials/custom_properties` warns about it. Found in stage 4b.
      **Owner: stage 6.**
- [ ] Halos of the `test_tipsy` test simulation cannot be loaded through
      `tangos write`. After `tangos add test_tipsy`, halo 1 has `NDM=2041986`
      (from the AHF stat file) in a snapshot of 3344 particles, and writing any
      particle property fails for every halo with `KeyError: 'No such halo 1'`
      from pynbody's halo catalogue -- although
      `tests/test_simulation_outputs.py` loads the same halos directly. Its
      trackers load fine, which is why `tests/test_docs_examples.py` writes the
      custom property example for a tracker. Either the fixture or the loading
      path deserves a look before anyone relies on the fixture for halo-level
      tests. Found in stage 4b.
      **Owner: stage 6.**
- [x] The web interface's merger tree failed for any simulation in which no
      object on the tree has `Mvir`, showing "Sorry, there was an error
      generating the tree": the values then come back as `None` rather than NaN,
      and `WebMergerTreeLayout.node_dict`
      (`tangos/web/views/merger_tree.py:50`) guarded only against NaN before
      formatting with `{:.2e}`. Every Gadget+SubFind simulation was affected,
      including `tutorial_gadget`. Found in stage 4.5, and **fixed in stage 4.5**
      at AP's request, with `test_merger_tree_without_Mvir` in
      `tests/test_web.py`; `tutorials/webserver` now shows `tutorial_gadget`'s tree.
- [ ] In the web interface's column headings, typing a number with an exponent
      and pressing Enter can replace part of it with an autocompletion
      suggestion: `mass<1e12` became `mass<1macros`. The autocompletion
      (`tangos/web/static/autocomplete.js`) treats the `e12` as the start of a
      name, and with `autoFocus: true` the first suggestion is highlighted, so
      Enter accepts it. `tutorials/webserver` tells readers to check the heading,
      and uses `VMax<300`. Suggesting names only after a letter that does not
      follow a digit would fix it. Found in stage 4.5.
      **Owner: stage 6.**
- [ ] `tangos serve --title` changes the breadcrumb trail but not the front page's
      heading, which is always "tangos on <hostname>"
      (`tangos/web/views/simulation_list.py:36` uses `socket.gethostname()`
      directly). Found in stage 4.5, whose screenshot of the front page leaves the
      heading out for this reason.
      **Owner: stage 6.**
- [ ] `/<simulation>/<timestep>/<x>/vs/<y>.csv?object_typetag=group` fails with
      `list index out of range` when no group has the properties asked for (on
      `tutorial_gadget`, `mass` against `VMax`), rather than returning an empty
      table or a clear error. Found in stage 4.5.
      **Owner: stage 6.**

## Misleading or wrong documentation

- [x] The tangos docs' own URLs percent-encode `+`, so four published page URLs
      cannot be served by Sphinx as-is. `builder.get_target_uri()` quotes the
      docname, so `first_steps_gadget+AHF` builds to
      `first_steps_gadget%2BAHF.html` while the Jekyll site publishes (and the
      README and any citing paper link to) the literal `+` spelling. Nothing
      warns: the build is clean under `-W` and the page is simply unreachable at
      its real URL. Stage 2 works around it for the *redirects* with a
      `build-finished` hook in `docs/conf.py`. The general lesson for later
      stages: never give a new page a name containing a character that
      `urllib.parse.quote` alters (the style conventions in `DOCS_PLAN.md`
      already require lowercase-underscore filenames, which is why only these
      retired pages are affected).
      **Closed in the stage 4 triage:** the redirects are worked around by the
      `build-finished` hook in `docs/conf.py`, and the lesson is recorded under
      **Invariants** in `DOCS_PLAN.md`.
- [x] `docs/Data exploration with python.ipynb` cell 1 links to
      `https://pynbody.github.io/tangos/data_exploration.html`, a page retired in
      stage 2. The redirect covers it, but the notebook is deliberately kept as
      the only published copy of material not yet ported (histograms, black
      holes, `match` crossmatching), so until stage 4 ports that material the
      notebook advertises a retired page. Sphinx does not warn, since it is an
      absolute URL. **Resolved in stage 3 by retiring the notebook**: since
      `docs-refactor-main` reaches readers only when the whole suite merges,
      keeping it bought nothing. The unported cells are pinned as a stage 4
      item in `DOCS_PLAN.md`.
- [ ] Two more doc URLs embedded in code will break when their pages move in
      stages 4-5, and have no redirect yet: `tangos/tools/property_writer.py:655`
      points at `custom_properties.html` and `tangos/core/halo.py:154` at
      `mpi.html`. Both pages are scheduled to move (`custom_properties` is ported,
      `mpi` becomes `how-to/parallel`). Add redirects as each moves, alongside the
      `input_handlers.html` dead link already recorded below.
      **Owner: stage 4.** The redirect for `mpi.html` lands with `how-to/parallel`
      (4a) and the one for `custom_properties.html` with the custom properties
      tutorial (4b). The URLs in the code can then stay as they are.
      The `mpi.html` redirect landed in 4a. In 4b, `custom_properties.html` kept
      its URL: the tutorial half moved to `tutorials/custom_properties`, but the
      reference half -- including the proxy material the `property_writer.py`
      message is about -- stays at `custom_properties.md` until stage 5 moves it
      to `reference/property_calculation_api`. **The `custom_properties.html`
      redirect is now stage 5's**, as part of that move.
- [x] `docs/custom_input_handlers.md:143` calls `.earlier` on the result of
      `get_halo(...)` (`tangos.get_halo("...").earlier.load()`). `earlier` is
      not an attribute of anything in the codebase; `.previous` is the
      attribute that walks one step back, and `earlier(n)` is a *live-calculation
      function* (a different namespace entirely). Fix when the input-handlers
      how-to is rewritten.
      **Owner: stage 4** (4a, `how-to/custom_handlers`).
      **Fixed in stage 4a**: `how-to/custom_handlers` uses `.previous`.
- [x] `docs/custom_input_handlers.md` presents `match_objects` as part of the
      `HandlerBase` contract, but `HandlerBase` does not define it at all — it
      is defined on `PynbodyInputHandler`, `YtInputHandler`,
      `Gadget4HBTPlusInputHandler` and the test handler only. Either add an
      abstract `match_objects` to `HandlerBase`, or correct the page to
      present it as specific to those subclasses.
      **Owner: stage 4** (4a, `how-to/custom_handlers`): present `match_objects` as
      what `tangos crosslink` needs from a handler, not as part of `HandlerBase`.
      **Fixed in stage 4a.**
- [x] `docs/first_steps_eagle.md:65` — the second `tangos import-properties`
      command reads `--for RefL0025N037`, missing the trailing `6`
      (`RefL0025N0376`, as used two lines above it).
      **Closed in the stage 4 triage:** the page was retired in stage 2, and
      `how-to/eagle` spells the name correctly throughout.
- [x] `docs/using_with_yt.md:85` writes a density profile to `tutorial_changa`,
      not `tutorial_changa_yt` (the simulation this tutorial actually adds).
      **Closed in the stage 4 triage:** the page was retired in stage 2, and
      `how-to/yt` writes to `tutorial_changa_yt`.
- [x] `docs/first_steps_ramses+hop.md:88` looks up
      `"tutorial/output_00010/halo_1"`, but the simulation added earlier in the
      same page (line 28) is named `tutorial_ramses`, not `tutorial`.
      **Closed in the stage 4 triage:** the page was retired in stage 2 and nothing
      carried the lookup over.
- [x] `docs/rdbms.md:34` — "For most users, MySQL and PostgreSQL are" — the
      sentence stops mid-way with no continuation.
      **Owner: stage 4** (4a, `how-to/database_backends`).
      **Fixed in stage 4a.**
- [ ] `README.md:10` claims tangos is "a modular system for Python 3.6+", while
      CI (`.github/workflows/build-test.yaml`) tests 3.11–3.14 only. (Checked
      `docs/index.md` too: it does not repeat this claim anywhere — it has no
      Python-version statement at all — so only `README.md` needs correcting,
      not both files as originally suspected.)
      **Owner: stage 6.** A one-line PR against master, since the README is what
      readers see now, long before the documentation merges.
- [x] `docs/mpi.md` still documents `pypar` as an alternative MPI backend to
      `mpi4py`. pypar is Python-2-era and unmaintained; see the corresponding
      dead-code entry below.
      **Owner: stage 4** (4a, `how-to/parallel`).
      **Fixed in stage 4a.**
- [ ] Halo-path syntax is inconsistent across the docs — some pages write
      `.../halo_1`, others `.../1` for the same object — and the equivalence is
      explained nowhere. (The curated API reference now explains it once, on
      `docs/reference/api/query.rst`; the tutorial pages themselves still need
      the sweep.)
      **Owner: stage 5.** Stage 4's new and ported pages use the canonical spelling
      as they go, so by stage 5 this should be confined to `live_calculation.md`.

- [ ] `TimeStep.trackers` also returns black holes, though nothing says so.
      `BH` subclasses `Tracker` (`tangos/core/halo.py:456`) and
      `TimeStep.trackers` (`:493`) is a polymorphic relationship on `Tracker`,
      so `BH` rows come back too. Reproduced on
      `tutorial_changa_blackholes/%960`: `trackers.count()` and `bhs.count()`
      are both 39, and `trackers.first()` is a `BH`.
      `docs/reference/api/objects.rst` lists `trackers` and `bhs` as sibling
      accessors, which reads as though they were disjoint. Either document the
      containment or give `trackers` a filter that excludes black holes.
      **Owner: stage 5.** If stage 4's tracking tutorial mentions `trackers`, it
      says that black holes are included.
- [x] `docs/rdbms.md` gives the wrong install command for the server backends:
      it says `pip install PyMySQL` / `pip install psycopg2-binary` rather than
      `pip install tangos[rmdbs]`, and omits the `[rsa]` extra that
      `setup.py:127` pins because MySQL 8's default `caching_sha2_password`
      auth needs it. A reader following the page as written can install
      PyMySQL and still fail to connect. This is separate from the truncated
      sentence on the same page recorded above. `docs/installation.rst` gives
      the correct form.
      **Owner: stage 4** (4a, `how-to/database_backends`).
      **Fixed in stage 4a.**
- [ ] The rmdbs extra should actually be rdbms (`pip install tangos[rdbms]`). Once
      fixed, references to `rmdbs` in the docs should be corrected.
- [ ] `tangos/input_handlers/__init__.py:7` sends readers to
      `https://pynbody.github.io/tangos/input_handlers.html`, which does not
      exist and never has — the page is `custom_input_handlers.html`. This is a
      dead link in shipped code, independent of the documentation rebuild.
      **Owner: stage 6.** Point it at `custom_input_handlers.html`, which works on
      the live site today; the redirect stage 4 adds when that page becomes
      `how-to/custom_handlers` carries it forward.
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
      **Owner: stage 5.**
- [x] `TimeStep.trackers` also returns black holes, but
      `docs/reference/api/objects.rst` lists `trackers` and `bhs` as sibling
      accessors, which reads as though they were disjoint sets. `BH`
      subclasses `Tracker` (`tangos/core/halo.py:456`) and `TimeStep.trackers`
      is a polymorphic relationship on `Tracker` (`:493`), so every `BH` row is
      returned by both. Confirmed on `tutorial_changa_blackholes/%960`:
      `trackers.count()` and `bhs.count()` are both 39, and `trackers.first()`
      is a `BH`. Either document the containment or give `trackers` a
      tracker-only variant.
      **Closed in the stage 4 triage as a duplicate of the item above.**

## Deprecated / vestigial

- [x] Consider changing `.keys()` on halos to return only one instance of each
      key, even where there are multiple links etc. This matches the existing dict-style
      behaviour: the key is passed *once* to `__getitem__`, the multiple values 
      are returned in one shot.
      **Closed in the stage 4 triage:** a design proposal rather than a defect.
      Raise it as a GitHub issue if it is still wanted.
- [x] **Replace all remaining `get_halo` references with `get_object`.** The
      project prefers `get_object` everywhere now (`get_halo` is kept as a
      working alias, not marked deprecated, because the tutorials still teach
      it exclusively). Grepping the docs directly:
      `docs/custom_input_handlers.md`, `docs/custom_properties.md`,
      `docs/first_steps_ramses+hop.md`, `docs/histogram_properties.md`,
      `docs/live_calculation.md`, `docs/tracking.md`, `docs/using_with_yt.md`
      (13 occurrences across those seven files; the 5 in the notebook went
      with it in stage 3). Deliberately **not** swept as
      a mass edit now — every one of these files is being rewritten in stages
      1–5 of `DOCS_PLAN.md`, so `get_object` replaces `get_halo` page by page,
      as each page is rewritten.
      **Owner: stage 4** for `custom_input_handlers.md` (4a) and
      `custom_properties.md`, `histogram_properties.md` and `tracking.md` (4b), the
      four files stage 4 ports. The item below covers `live_calculation.md`.
      `docs/reference/api/query.rst` is out of scope: it documents `get_halo`
      itself. `custom_input_handlers.md` was done in 4a; `custom_properties.md`,
      `histogram_properties.md` and `tracking.md` in 4b (the first keeps its
      reference half, now using `get_object`).
- [ ] The same `get_halo` → `get_object` sweep for `docs/live_calculation.md`
      (one occurrence), split from the item above in the stage 4 triage.
      **Owner: stage 5**, which retires that file into
      `reference/live_calculation_language`.
- [x] `config.enable_async_message_processing` should not be advertised, even
      on the forthcoming `configuration` reference page. Its own comment in
      `tangos/config.py` says it "can lead to subtle race conditions", "may
      (?) also be implicated in hangs", that the performance benefit is
      unclear, and that it "may be that async processing should be removed
      from the codebase entirely". Default is `False`; leave it undocumented.
      **Closed in the stage 4 triage:** this is a standing instruction, not a fix,
      and now lives under **Decisions already made** in `DOCS_PLAN.md`.
      `docs/configuration.rst` already omits the setting.
- [x] The five deprecated console-script shims —
      `tangos_writer`, `tangos_crosslink`, `tangos_timelink`, `tangos_add_bh`,
      `tangos_import_from_ahf` (`setup.py`'s `console_scripts` entry points,
      forwarding to `tangos.scripts.{writer,crosslink,timelink,add_bh,import_from_ahf}.main`)
      — each prints a deprecation notice and forwards to the `tangos`
      subcommand. Candidates for removal once nothing in the wild depends on
      them; in the meantime, do not resurrect them as documented commands in
      `reference/cli`.
      **Closed in the stage 4 triage:** "do not document them" now lives in the
      stage 5 `reference/cli` item in `DOCS_PLAN.md`. Removing them is a release
      decision, not a documentation fix.
- [ ] `tangos/parallel_tasks/backends/pypar.py` is dead: pypar has not been
      maintained since the Python 2 era. `docs/mpi.md` still lists it as an
      alternative to mpi4py (see the docs entry above). Candidate for deletion
      alongside that doc fix. `tangos write --help` also still offers it, in
      `--backend`'s help text ("e.g. pypar, mpi4py").
      **Owner: stage 6.**
- [ ] Integer halo (and other object) properties are stored as 32-bit except on
      sqlite; probably unintentional and should be fixed provided we can retain
      backwards compatibility with databases that have the 32-bit column.
      **Owner: stage 6.** Needs a design for existing databases with 32-bit columns
      before any code changes.
