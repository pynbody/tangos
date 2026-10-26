# Documentation overhaul: plan and progress

The tangos documentation is being rebuilt, in stages, from a Jekyll site of loose
markdown files into Sphinx documentation on Read the Docs, following pynbody's setup
closely so the two projects' documentation shares a look and feel.

The work spans many pull requests. **This file is the source of truth for both the
plan and the progress**: each PR ticks its own boxes in the same commit that does the
work, so status cannot drift from reality.

## How to work on this

Stages merge progressively into the long-lived `docs-refactor-main` branch, not into
`master`:

```
git fetch origin docs-refactor-main
git checkout -b docs-stage-N origin/docs-refactor-main
```

Title PRs `docs: stage N — <thing>` and label them `docs-overhaul`. Do one stage per
PR and merge it before starting the next: almost every stage touches the `index`
toctree, and stage 2's deletions collide with anything else editing those files.
`docs-refactor-main` merges into `master` once the suite is complete.

Read **Decisions already made** before changing anything under `docs/`. Several of
those entries look like omissions or mistakes and are not.

Things you find that need fixing in the *code* rather than the docs go in
`KNOWN_ISSUES.md` at the repository root.

Build the docs with:

```
pip install -e .[docs]
python -c "import tangos.test_utils as tu; tu.precache_tutorial_database()"
cd docs && python -m sphinx -W -b html . _build/html
```

It takes a few minutes. `-W` matches CI: the build is warning-free and must stay so.

**Do not skip the `precache_tutorial_database()` line.** It fetches the 1.2 GB
tutorial database to `~/tangos_data.db`, which is what every `.. ipython::` block on
the tutorial pages queries, and it is what `pre_build` in `.readthedocs.yaml` runs.
Since stage 3, `docs/conf.py` refuses to build without it: it checks, read-only, that
the database exists and holds the five tutorial simulations. Before that, querying a
database that does not exist silently created an empty one (see `KNOWN_ISSUES.md`),
and the pages rendered with empty output while `-W` stayed green.

**Run `pre-commit run --all-files` before you push**, and commit anything it changes.
The hooks run in CI and will fail the build otherwise. isort in particular reorders
imports in `docs/conf.py`, which is easy to miss because nothing in the documentation
build itself complains.

## Status

Stages 0, 1, 2 and 3 are complete; stages 4–6 have not started. Stage 0 landed in
#296, stage 1 in #300, stage 2 in #305, stage 3 in #312.

Every page from stages 0–2 has now been read by a human. Stage 3 changed the
examples on `tutorials/live_calculations` after its check, and says so in a second
comment line under the check line; those changes still want a human read. A
`.. Checked by <initials> <date>` line is the only record of which pages have had
human eyes on them, so do not let a page lose it in a rewrite, and when you change
a checked page, say what changed beneath it.

One thing outside this repository is still outstanding, and it cannot be done
from a checkout: the Pages setting has not been checked (**Before merging to
master**). The tutorial data is no longer among them — every dataset the docs
link to is now on the server; see the first item under **Stage 3**.

## Stages

Effort estimates are person-days. **Stages 1–2 alone ship a coherent site**; the rest
deepens it.

### Stage 0 — build system (complete)

What now exists, as context for later stages:

- Sphinx with `sphinx_book_theme`, configured in `docs/conf.py` closely following
  pynbody's. `myst_parser` renders the existing `.md` pages unchanged; `nbsphinx`
  rendered the notebook until stage 3 retired both. The page structure is still the old one, ported as-is.
- tangos palette and wordmark in `docs/_static/custom.css`; `.readthedocs.yaml`;
  `docs` extra in `setup.py`; `docs/Makefile`.
- A curated API reference: 11 audience-organised pages under `docs/reference/api/`,
  hand-written `autoclass`/`autofunction` stanzas. This replaced a recursive
  `autosummary` version that generated 657 near-empty stubs.
- A temporary site-wide banner saying the docs are being rebuilt, linking to the old
  site. See **Before merging to master**.
- Extensions later stages depend on, installed and verified: `sphinx-design` (stage
  2's tabs), `sphinx-argparse` (stage 5's CLI reference), and the `.. ipython::`
  directive together with pynbody's logging filter that turns an unmarked exception
  inside such a block into a build failure (stage 3).
- Zero warnings, with `-W` enforced via `fail_on_warning: true` in
  `.readthedocs.yaml`.

### Stage 1 — the newcomer path (complete)

Essentially all of the newcomer benefit is concentrated here. Two problems drive it:

*The path is inverted.* `index.md` sends a first-time reader to `first_steps.md` —
build a database from multi-GB raw data, four CLI commands, requires pynbody —
before `data_exploration.md`, which merely queries an existing sqlite file. The
shortest documented route from `pip install` to a first plot is measured in hours.
Flipping it costs little, because the content already exists inside the notebook.

*There is no explanation tier at all.* The docs are roughly 55% tutorial, 35%
how-to, 10% reference, 0% explanation. Nothing explains the data model — Simulation
→ TimeStep → object typetags (`halo`/`group`/`BH`/`tracker`/`phantom`) → properties
→ links → creators — before the commands start. Every page silently assumes it.

- [x] `explanation/concepts` (new, ~150 lines) — the data model. Highest
      value-per-line item in the whole plan
- [x] `index` — rewrite as a real landing page
- [x] `installation`
- [x] `configuration` (new) — `config_local.py` is documented in `tangos/config.py`
      and nowhere in `docs/`
- [x] `tutorials/quickstart` — from notebook cells 0–16
- [x] `tutorials/time_series` — from notebook cells 17–19, 28–32
- [x] `tutorials/live_calculations` — from `live_calculation.md:1-346`
- [x] `tutorials/webserver` — grow the 21-line stub
- [x] Start the redirect map (see **Invariants**). URLs first move here; stage 2
      moves many more and continues the same map

These were written as `ipython` blocks throughout. All 20 notebook code cells
recalculate cleanly against the sample database, so nothing had to be dropped.

What stage 1 did beyond the list above, because the executable blocks needed it:

- Added `tangos.test_utils.precache_tutorial_database()` — brought forward from
  stage 3, since RTD cannot build executable tutorials without the database.
- `pre_build` in `.readthedocs.yaml` calls it. Measured: the download takes 25 s
  for 1.28 GB from the r2 bucket, and a clean local build takes 17 s.
- Added `sphinx-reredirects` and the redirect map in `docs/conf.py`. Only one URL
  retires in stage 1 (`data_exploration_webserver.html`), but the mechanism is
  now proven end-to-end, so stage 2's six deletions inherit something that works.
- Added `pynbody` to the `docs` extra. See **Decisions already made**.
- `docs/plots/` (the `@savefig` working directory) is gitignored. The `@savefig`
  images land in `_images/` correctly, so pynbody's `post_build` copying step
  turned out not to be needed; a note in `.readthedocs.yaml` says to try it if
  images ever go missing on RTD.


### Stage 2 — collapse the first_steps combinatorics (complete)

The six `first_steps_*` pages are ~70% byte-identical — the `--with-prerequisites`
bullet list is word-for-word across subfind, rockstar and ramses. Only eight things
vary, and only **two** branch the command *sequence*: whether the finder supplies its
own merger trees, and whether the format has a group/halo hierarchy.

- [x] `tutorials/first_database` — one canonical page on **ChaNGa+AHF**: the only
      dataset with baryons, so `uvi_image`/`SFR_histogram` work and everything
      downstream depends on them, and it has the black-hole companion run
- [x] `sphinx-design` `tab-set` with `:sync:` keys at exactly two steps — "import
      finder properties" and "build the merger tree" — so choosing a finder once
      reconfigures the page. Every other step stays single-path prose. Tabs cover
      AHF, SubFind, Rockstar, HOP and HBT+; the AHF tab is the path the page's own
      dataset takes, the rest say what to type for a simulation of your own
- [x] `reference/simulation_formats` — all combinations as flat rows, modelled on
      `pynbody/docs/loaders.rst:10-22`. 17 rows, plus a second table of tutorial
      datasets marking which are built by `build.sh` (that second table was cut
      when the page was checked)
- [x] Keep EAGLE and yt as separate short pages; their differences are not in the
      command sequence. Both light-ported to rst under `how-to/`
- [x] Retire `old.md`, `advanced.md`, `first_steps.md`, `data_exploration.md`,
      `bluewaters.md`, `troubleshooting.md`. Leave the Jekyll files until the Pages
      question in **Before merging to master** is settled. Also retired:
      `data_exploration_webserver.md`, which stage 1 replaced but left on disk and
      in the toctree while already redirecting its URL, so the built page was being
      overwritten by a redirect to its own replacement
- [x] Redirects for every deleted URL

The tradeoff of tabs is real: they defeat Ctrl-F and PDF output, and deep-linking
gets worse. That is why they are restricted to two steps and backed by a searchable
flat table.

What stage 2 found that the next stages need to know:

- **A `+` in a docname silently breaks its redirect.** Sphinx percent-encodes the
  docname in `get_target_uri()`, so sphinx-reredirects wrote
  `first_steps_gadget%2BAHF.html` while the published URL has a literal `+`. The
  build stayed clean under `-W`; four of the six retired URLs would simply have
  404ed. `docs/conf.py` now has a `build-finished` hook that also writes each stub
  at the unquoted name. **Check redirects by listing the built files, not by
  trusting a green build** — nothing warns.
- **The stage 3 `build.sh` test needs an allowlist.** `reference/simulation_formats`
  documents `import-ahf-trees` and `import-subfind-trees`, correctly — they are the
  merger-tree route for two supported combinations — but neither appears in
  `build.sh`, which has no `build_gadget_ahf` and builds no tree at all for
  Gadget-4. So the test as specified would fail on a page that is right. Either
  `build.sh` grows those cases, or the format reference is exempt from the check.
  **Resolved in stage 3** with an allowlist keyed by subcommand and flag, each
  entry with its reason; growing `build.sh` would change the database that
  `tangos diff` compares against `reference_database.db`.
- The Gadget-4 merger tree is entirely unverified, and `import-subfind-trees` has no
  test, no `build.sh` line and no page. Recorded in `KNOWN_ISSUES.md`; the format
  reference says so on its face.

### Stage 3 — make the examples executable and verified (complete)

tangos cannot do directly what pynbody does. pynbody's build-time dependency is a
snapshot file it reads itself; tangos needs a *populated database*, meaning pynbody +
yt + particle data + hours of `tangos write` (`build.sh` says 35 GB). But that
database is **already built on every PR**:
`.github/workflows/integration-test.yaml` runs `INTEGRATION_TESTING=1 bash build.sh`
against the mini datasets under `https://tangos.pontzen.uk/continuous_integration/`,
uploads `data.db` as an artifact, and verifies it
with `tangos diff`. So decouple database *production* from docs *building*.

- [x] **Needed a human first**: put the much larger *source files* — the raw
      simulation data and catalogues needed to recreate the tutorial database —
      somewhere permanent, and give it a public URL in place of the development
      one (`pub-d85a828023a1452bbd3a294bc72003f0.r2.dev`). **Done**: everything
      now lives under `https://tangos.pontzen.uk/`, with the full tutorial data
      under `/tutorials/` and the cut-down CI fixtures under
      `/continuous_integration/`. The non-docs half of that move (`build.sh`,
      `integration-test.yaml`) went to master separately in #310, which merged;
      the docs half (`TUTORIAL_DATABASE_URL`, `docs/installation.rst`, every
      dataset link in `reference/simulation_formats`) is on this branch.

      **All seven tutorial datasets are now present under `/tutorials/`**,
      verified over HTTP on 2026-10-24: `tutorial_changa`,
      `tutorial_changa_blackholes`, `tutorial_changa_yt`, `tutorial_gadget`,
      `tutorial_gadget_rockstar`, `tutorial_gadget_ahf` and `tutorial_ramses`.
      `tutorial_gadget` was the last gap, and was a mis-upload rather than a
      missing file: it sat in the bucket as `tutorial_gadget.tar`, uncompressed
      and without the `.gz` suffix that `build.sh` and
      `reference/simulation_formats` both expect. A one-off GitHub Actions job
      (#311, closed rather than merged, since it only ever needed to run once)
      checked the archive, gzipped it, uploaded `tutorial_gadget.tar.gz` and
      deleted the original. Confirmed afterwards by streaming the live URL
      through `curl … | tar -tz`: 89 members, 11 snapshots, clean exit. That
      mattered beyond one dataset, because `tutorial_gadget_rockstar` and
      `tutorial_gadget_ahf` are *catalogue-only* tarballs that need the
      snapshots from `tutorial_gadget` beside them.

      `tutorial_gadget4`, `tutorial_gadget4_hbtplus` and `enzo.tinycosmo` do
      404 under `/tutorials/`, but **that is correct and needs no upload**: no
      full dataset for them has ever existed. Each was introduced purely as an
      integration test fixture — "Add integration test for gadget 4 / HDF
      format" (2022-07-05), "Add integration test for improved yt
      functionality" (2022-09-14), "Add HBT+ mini-test set" (2024-06-20) — and
      no documentation page has ever offered one for download; the only commit
      in the whole history naming them under `docs/` is stage 2's own. They
      exist solely as cut-down fixtures under `/continuous_integration/`.
      `reference/simulation_formats` was initially wrong about this, inferring
      `/tutorials/` URLs for them from `build.sh`'s pattern, and now says
      plainly that the three have no tutorial dataset.

      One distinction to keep straight, because it is easy to overstate a gap
      here. `build.sh` chooses its path by `INTEGRATION_TESTING`:
      `/continuous_integration/` when set, `/tutorials/` when not. **All eight
      datasets it names are present under `/continuous_integration/`**, as is
      `reference_database.db`, and #310's integration test built the whole
      database and passed `tangos diff` against it. So a gap under
      `/tutorials/` never blocks CI; it blocks the human path, someone running
      `build.sh` for real over the 35 GB route.
- [x] Add `tangos.test_utils.precache_tutorial_database()`, a direct analogue of
      `pynbody.test_utils.precache_test_data()` — **done in stage 1**, which
      needed it to build executable tutorials at all. It carries the same "must
      not depend on a working installation" contract as pynbody's, since RTD
      calls it from `pre_build`. It still replaces the four hand-written
      Zenodo-URL paragraphs pointing at the 2021 database; that sweep happens as
      each page carrying one is rewritten.
- [x] `pre_build` in `.readthedocs.yaml` downloads only that sqlite file — **done
      in stage 1**. Measured rather than budgeted: 25 s download, 17 s for a
      clean local build of the whole site. Still excluded: yt, particle data, any
      `tangos write`. pynbody is now installed, contrary to the original budget —
      see **Decisions already made**.
- [x] Convert tutorial code fences to `.. ipython::` / `@savefig`. Scoped to what
      runs against the tutorial database on pages already ported: the six
      hand-typed blocks on `tutorials/live_calculations`. Every other code block on
      a stage 1–2 page is shell, a config file, or needs data the build does not
      have (yt, EAGLE). The `.md` pages stages 4–5 rewrite were left alone, not
      converted in place only to be re-ported
- [x] Bind the write-side shell commands to `build.sh` — a test comparing
      subcommands and flags, not whole command lines (see **Invariants**: the docs
      deliberately differ from `build.sh` in argument *values*). Sharing literal
      `_snippets/*.sh` via `literalinclude` was the original idea and is ruled out
      by that: it would force the docs to adopt CI's values. They are already
      CI-verified; the docs simply do not reuse that fact. Done as
      `tests/test_docs_commands.py`, which parses with the real parser rather than
      grepping; see **What stage 3 found**
- [x] Fail the build if the tutorial database is absent or empty (added in
      stage 3; see **How to work on this**)
- [x] Retire the notebook, and `nbsphinx` and `pandoc` with it. Its URL
      redirects to `tutorials/quickstart`; the cells nothing has ported yet are
      pinned under **Stage 4**
- [x] Re-enable the integration test workflow (see **Before merging to master**) —
      this stage depends on the database it builds, so restore it here rather than at
      the end. Done: the file is now byte-identical to master's

Fallback, worth adopting as a floor regardless: `SimulationGeneratorForTests` builds
8 timesteps × 9 halos with links in 1.7 s, with no pynbody and no data. That alone
keeps the language reference executable if a download fails. **Deferred to stage
5**, where the language reference it would serve is written; stage 3 instead
makes a missing database a hard failure, since a fallback would render the
tutorial pages with meaningless output.

What stage 3 found:

- **An empty result is not verification.** Of the ten hand-typed calls on
  `tutorials/live_calculations`, three raised, and four returned empty arrays
  without complaint: `later(5)` from the final snapshot and `earlier(10)` in a
  simulation with five. An `.. ipython::` block of those builds cleanly and proves
  nothing. Look at the output of every block you add, and choose arguments that
  return real data. The tutorial simulations have five snapshots each.
- **Under `-W`, an unmarked python warning fails the build too**, not only an
  unmarked exception: the ipython directive reports both as Sphinx warnings. Mark a
  block `:okwarning:` when its warning is expected. The exception filter in
  `conf.py` is what makes an unmarked exception fatal *without* `-W`. Both were
  checked with deliberately failing blocks.
- **How the command test works**, since stages 4–5 will meet it. It collects
  every `tangos …` line from shell-like code blocks on every `.rst`/`.md` page
  (rst `code-block`s, `::` literal blocks, markdown and MyST fences), and every
  `tangos` line of `build.sh` with `$MPI…` expanded for both branches of
  `detect_mpi`. It parses them all with `tangos.scripts.get_argument_parser()`,
  added in this stage. Flags are compared by argparse dest, so `--for` and
  `--sims` match, and so do abbreviations. A documented command must parse, its
  subcommand and flags must appear in `build.sh` or in `ALLOWLIST`, and an
  allowlist entry nothing needs fails as stale. A placeholder line, one with
  `<N>` or `[…]` in it, is skipped. The allowlist has six entries, each with its
  reason: `serve`, `recent-runs`, `import-ahf-trees`, `import-subfind-trees`,
  `add --max-objects` (EAGLE), and `import-changa-bh --backend` (`mpi.md` runs
  it under MPI, while `build.sh` runs it serially).
- `match_reduce` crashes if a linked object lacks the property, and `--backend`
  handling re-splits the command line on spaces. Both are in `KNOWN_ISSUES.md`.
  The second matters to any page that shows `--backend` together with an
  `--include-only` containing a space.

### Stage 4 — gap-filling how-tos and remaining tutorials (5 d)

- [ ] **First, triage `KNOWN_ISSUES.md`** (under a day). Stages 0–3 only ever
      added to it: at the end of stage 3 it held 43 open items (21 broken code,
      16 misleading documentation, 6 deprecated/vestigial) and nothing assigned
      them to anyone. Tag every open item with one owner, in the item itself:
      - **stage 4** or **stage 5** — a docs error on a page that stage rewrites
        (e.g. `custom_input_handlers`, `rdbms`, pypar in `mpi.md`, and the doc
        URLs embedded in code, for stage 4; halo-path spelling, the two
        `trackers`/BH items and runtime-changeable config, for stage 5). The
        stage fixes it as part of the rewrite and ticks it;
      - **stage 6** — a code bug or dead code (see **Stage 6**);
      - **close** — stale or not worth doing. At least three are already fixed
        and only want ticking: the `first_steps_eagle`, `using_with_yt` and
        `first_steps_ramses+hop` items, whose pages stage 2 retired without
        carrying the errors into `how-to/eagle` or `how-to/yt` (checked in
        stage 3). Design notes such as the `.keys()` one are also candidates.

      Where a code bug changes what a stage 4 or 5 page should say — the
      `--backend` re-splitting bug for `how-to/parallel`; the empty-database
      creation, `get_simulation`'s `_` wildcard, `getdb` and
      `tangos.core.__all__` for the API reference — note on the item that the
      page depends on its fix, so the page documents the fixed behaviour or
      flags the bug rather than enshrining it
- [ ] `how-to/managing_a_database` (new) — the biggest single gap
- [ ] `how-to/large_simulations` (new)
- [ ] `how-to/parallel`, `how-to/database_backends`
- [ ] `how-to/custom_handlers`, absorbing `how-to/yt` (moved there in stage 2).
      That retires `how-to/yt.html`, a URL stage 2 created, so it needs its own
      redirect and the `using_with_yt` entry in the map needs repointing
- [ ] Port `custom_properties`, `histograms`, `black_holes`, `crossmatching`, `tracking`
- [ ] Carry over the notebook cells stage 1 did not port. Stage 3 retired
      `Data exploration with python.ipynb`; recover it with
      `git show "101890c:docs/Data exploration with python.ipynb"`. Still unported:
      cells 20–26 (the SFR histogram plotted against its time bins via
      `x_values()`; the fraction of star formation in the major progenitor, with
      the note on merger quantisation at output times; the BH accretion rate) →
      `histograms`; cells 39–40 (SFR with and without AGN via
      `match('tutorial_changa').SFR_histogram`) → `black_holes`/`crossmatching`.
      Everything else in it is already in `quickstart`, `time_series` or
      `live_calculations`
- [ ] Write every new page's examples as `.. ipython::` from the start, run against
      the tutorial database (see **What stage 3 found** for what "verified" takes).
      Pages whose examples write to the database (`tracking.new`,
      `sim[...] = ...` on the histogram page) must not mutate the shared
      `~/tangos_data.db`: work on a copy, or show those blocks unexecuted
- [ ] Web interface, currently a 21-line stub. Undocumented: arbitrary
      live-calculation expressions as timestep-view columns (arguably the killer
      feature), the merger-tree viewer, and the entire URL API in
      `tangos/web/routes.py` — `.../{x}/vs/{y}.csv`,
      `.../gather/{typetag}/{name}.json`, `cascade_plot`
- [ ] The four supported code × finder combinations with no page at all:
      Gadget-4+SubFind, Gadget-4+HBT+, Ramses+AdaptaHOP, Enzo+Rockstar-via-yt. Two are
      built by `build.sh` on every PR.

      **None of them has data a reader can download**, so none can take the
      shape of the other tutorials: there is no "fetch this tarball" step to
      write. Gadget-4+SubFind, Gadget-4+HBT+ and Enzo+Rockstar-via-yt exist only
      as integration test fixtures (see **Stage 3**), and Ramses+AdaptaHOP has no
      dataset at all. Either publish real datasets first, or write these as
      reference material addressed to someone who already has their own data —
      which is what `reference/simulation_formats` currently does for them, and
      may be all they need.
- [ ] `how-to/parallel` (from `mpi.md`): its list of commands eligible for
      parallel execution omits `import-changa-bh`, which its own PBS example runs
      under MPI, and which is parallel-aware
- [ ] `--pickle-results` — the actual answer to the sqlite-locking failure that
      `rdbms.md` warns about, undocumented

### Stage 5 — reference (4 d)

- [ ] `reference/cli` — **26 CLI subcommands exist, roughly 9 are documented**.
      Missing: `import` (merge a sqlite file into a server, the obvious sequel to
      `rdbms.md`), `rollback`, `recent-runs`, `delete-properties`, `thin-timesteps`,
      `diff`, `prune`/`patch-trees`, `write-pickled-results`
- [ ] `reference/live_calculation_language` — from `live_calculation.md:347-652`
- [ ] `reference/property_calculation_api` — from `custom_properties.md:218-312`
- [ ] `reference/builtin_properties` — a table of the ~60 names, not class pages
- [ ] Decide the reference's top level: `reference/api/index` is currently the sole
      entry point. Once the pages above exist, either add a `reference/index` landing
      page over them or keep `api/index`'s toctree as-is
- [ ] Convert to real cross-references the `:doc:`/`:ref:` mentions left as plain text
      in `reference/api/` because their targets did not exist: `reference/cli`,
      `reference/live_calculation_language`, `reference/builtin_properties`,
      ~~`reference/simulation_formats`~~ (done in stage 2, along with
      `/first_steps_eagle` → `/how-to/eagle`), `configuration`,
      `explanation/concepts`. Do each as the page it names lands
- [ ] FAQ, link audit

### Stage 6 — code fixes from `KNOWN_ISSUES.md` (size set by the stage 4 triage)

The items the stage 4 triage tags **stage 6**: bugs and dead code found while
writing the docs. Unlike every other stage, this one **does not go through
`docs-refactor-main`**. Each fix is a small PR against `master`, as #308 and #310
were, reviewed as code and released on its own schedule rather than held until
the whole documentation merges. `docs-refactor-main` picks the fixes up when it
next merges `master`.

- It can start as soon as the triage is done, and run alongside stages 4–5.
- One fix per PR, each with a test, and ticked in `KNOWN_ISSUES.md` in the same
  PR. Not a mass sweep: `get_halo` → `get_object` stays page by page, as
  **Decisions already made** says.
- It gates the final merge only where a page depends on a fix (the triage marks
  those); everything else may finish after the docs ship.

## Target architecture

**Setup** — `index` · `installation` · `configuration`

**Tutorials**, linear and in this order. The first five need no simulation data,
and no pynbody except for the one metadata call noted below: `explanation/concepts` → `tutorials/quickstart` → `tutorials/time_series`
→ `tutorials/live_calculations` → `tutorials/webserver` → `tutorials/first_database`
→ `custom_properties` → `histograms` → `black_holes` → `crossmatching` → `tracking`

**How-to** — `parallel` · `managing_a_database` · `database_backends` ·
`large_simulations` · `custom_handlers` · `eagle`

**Reference** — `simulation_formats` · `live_calculation_language` · `cli` ·
`property_calculation_api` · `builtin_properties` · `api/index`

## Decisions already made

### About the content

- **Prefer `get_object` over `get_halo` everywhere.** Both work and both are in the
  reference, but `get_object` is the one new prose should call. The existing pages use
  `get_halo` throughout; that sweep happens as each page is rewritten, not as a mass
  edit, and is tracked in `KNOWN_ISSUES.md`.
- **The docs may differ from `build.sh` in argument *values*, never in command
  *shape*.** `build.sh` exists to build a test database quickly on CI hardware, not
  to teach; its choices are tuned for that. `tutorials/first_database` therefore
  keeps `--include-only="NDM()>1000"` where `build.sh` filters at 5000, because a
  1000-particle cut is the one a reader should actually learn. The stage 3 test
  enforces the shape (subcommand and flags), not the numbers — see **Invariants**.

- **The notebook is converted to `.rst` and retired, not kept.** Its content is the
  best material tangos has; its container is failing it — reachable only via an
  nbviewer link, invisible to site search, 278 KB of undiffable base64, unchanged
  since 2022, and every calculation uses the string form that `live_calculation.md`
  now de-emphasises. Converting is the moment to rewrite all 20 cells in lambda form.
- **`live_calculation.md` splits at line 347; do not rewrite it.** It was rewritten in
  August 2026 for the 1.12.0 lambda work and is the best file in the docs: tutorial
  above that line, reference below. `custom_properties.md` splits the same way at line
  218, where `region_specification`/`preloop` stop being a learning path and become
  API contracts.
- **`old.md` is retired, but salvage three fragments first**: `:23-37` is the only
  documentation anywhere of `recent-runs`; `:199-207` has better `at()` examples than
  the current pages; `:106-126` is a real PBS script. It is otherwise the pre-1.0
  README of `halo_database` and nothing links to it.

- **pynbody is required to build the docs**, though not to read a database.
  Asking a property to describe itself — `get_description(...)`, `x_values()` —
  constructs the simulation's input handler, which imports pynbody, while reading
  a stored value does not. `tutorials/quickstart` makes one such call, so pynbody
  is in the `docs` extra. It is a pure wheel (10 MB, no compilation), so this
  costs a download. The underlying design issue is in `KNOWN_ISSUES.md`; if it is
  ever fixed, this dependency can go.
- **A live calculation's parentheses are not decoration.** A bare name is a
  *stored* property and a name with `()` is a *live-calculation function*:
  `halo.calculate(lambda: Mvir)` returns a mass, while `lambda: Mvir()` raises
  `NameError: No providing class for property Mvir`. Existing prose in
  `live_calculation.md` uses `Vvir()` as its lambda example, and that particular
  call fails against the tutorial database. Run every calculation before
  documenting it.
- **`explanation/concepts` owns the halo-path spelling**, not `tutorials/quickstart`
  as first planned. It explains the three parts, the `typetag_number` component,
  the bare-number shorthand and what `%` really is. Other pages use paths freely
  and link to `:ref:`concepts`` rather than re-explaining. `reference/api/query.rst`
  still carries its own syntax table, which is fine as reference but should
  cross-reference rather than compete.
- **Implementation detail is deferred and labelled, on every page.** A newcomer
  must not have to read internals to reach what they came for: table names,
  columns, typecodes and ORM mechanics go in a trailing section a reader can skip
  ("Under the hood: how this is stored" on `explanation/concepts`, "All settings"
  on `configuration`), and the page must still leave a correct mental model if
  they stop before it. `configuration`'s reference half is about half that page,
  which is more than the rule of thumb allows but reads fine because the tables
  are skimmable; splitting it into a `reference/` page remains an option.
- **New pages carry `.. Generated by Claude on <date>; not yet checked by a
  human.`** rather than pynbody's `.. Last checked by AP: <date>`, which would
  claim a check that has not happened. Replace the line when you check the page.

### About the build

- **`conf.py` must call `configure_mappers()` after importing tangos.**
  `Simulation.timesteps`, `SimulationObjectBase.links`/`reverse_links` and
  `Simulation.trackers` are SQLAlchemy backrefs that do not exist on the class until
  mappers are configured. Without it autodoc silently omits exactly the object-model
  relationships the reference exists to document, and does not warn.
- **`autosummary` and `autoclass`/`autofunction` resolve dotted names differently.**
  Under a `currentmodule`, autosummary prepends the module to a partial dotted name;
  autodoc's own directives do not, and silently mis-resolve rather than erroring.
  Expect to hit this when adding stage 5's pages.
- **`autodoc_default_options` stays unset**, as in pynbody. Setting it makes autodoc
  expand every member of anything documented, duplicating explicit stanzas.
- **The sidebar needs both `logo.text` and CSS, unlike pynbody.** sphinx-book-theme
  renders the brand in the *sidebar* as a vertical stack with no height cap. That
  suits pynbody, whose logo is a wide banner with the wordmark drawn into the artwork.
  The tangos logo is a portrait glyph, which the same rules blow up to the full
  sidebar width, and carries no wordmark. So tangos sets
  `html_theme_options["logo"]["text"]` and restores a capped horizontal row in
  `custom.css`. Do not "simplify" either half towards pynbody's config.
- **Do not copy pynbody's `docs/_templates/`, `_static/alabaster.css`,
  `docs/tutorials/example_code/`, or `matplotlib.sphinxext.plot_directive`** when
  syncing against it. All are dead in pynbody itself: templates referencing
  stylesheets that do not exist and overriding a block the theme does not define,
  a stylesheet for a theme neither project uses, 13 scripts nothing references, and an
  extension loaded although no `.. plot::` appears anywhere.
- **tangos' packaging is `setup.py`, not `pyproject.toml`**, unlike pynbody. The
  `docs` extra lives there; `pyproject.toml` has no `[project]` table.
- **`sphinx-book-theme` is pinned to 1.2.0**, matching pynbody: 1.3.0 pulls in
  `pydata-sphinx-theme>=0.17`, whose sidebar rework breaks the primary sidebar on
  narrow viewports.
- **No notebooks, so no `nbsphinx` and no `pandoc`.** Both were build
  dependencies only for `Data exploration with python.ipynb`, retired in stage 3
  (see the first item under **Decisions already made**). Write any future tutorial
  as `.rst` with `.. ipython::` blocks rather than reintroducing a notebook.
- **`index.md` is currently the Sphinx root doc**, with a MyST `{toctree}` appended,
  rather than a separate `index.rst`. That suited a placeholder porting markdown in
  place; stage 1 rewrites it.

## Before merging to master

- [ ] **Check Settings → Pages before deleting any Jekyll file** (`_config.yml`,
      `Gemfile`, `_layouts/`, `_sass/`). No workflow deploys the current site, so
      Pages is almost certainly configured to serve Jekyll straight from `docs/` on
      `master` — a setting invisible from a checkout. Deleting `_config.yml` while
      that is still set would break the live site, so the Pages source must be
      switched or disabled at the same moment.
- [x] **Re-enable the integration test workflow**: uncomment `pull_request:` in
      `.github/workflows/integration-test.yaml`. **Done in stage 3**, leaving the
      file byte-identical to master's, so the hazard below should no longer
      arise; still check the merged file rather than assuming. It was disabled for the rebuild
      because it builds a test database from real simulation data on every PR, which
      no docs change warrants. Stage 3 needs it back earlier than this.

      **This is now a merge hazard, not just a chore.** The trigger is commented
      out *only on this branch*: master still has `pull_request:` live, and has
      used it (it is what verified #310's URL move end to end). So a merge of
      `docs-refactor-main` into master takes this branch's version of the file and
      silently *disables* the integration test on master — master never touched
      that line, so git resolves it in this branch's favour without a conflict and
      without comment. Uncomment it before the merge, not after, and check the
      merged file rather than assuming.
- [ ] **Remove the rebuild-in-progress banner**:
      `html_theme_options["announcement"]` in `docs/conf.py` and the
      `.bd-header-announcement` rules in `docs/_static/custom.css`. It points readers
      at the old site, which merging this work retires.

## Invariants

These span PRs and are easy to break one stage at a time.

- **Every deleted page URL gets a redirect.** The six `first_steps_*.html` URLs are
  linked from the README and probably from published papers.
- **Verify redirects by listing the built files, never by trusting a green build.**
  Sphinx percent-encodes the docname in `get_target_uri()`, so sphinx-reredirects
  writes its stub at the *encoded* path: four of stage 2's URLs contain a literal
  `+` and silently got stubs at `first_steps_gadget%2BAHF.html` and friends, which
  no reader's link resolves to. Nothing warned — the build was clean under `-W` and
  the URLs would simply have 404ed. `docs/conf.py` now carries a `build-finished`
  hook that writes each stub at the unquoted name too, so existing redirects are
  safe; the trap is live for any *new* redirect whose docname contains a character
  `urllib.parse.quote` alters. The style conventions already forbid such characters
  in new filenames, so this should only ever bite on retired pages.
- **`sphinx-build -W` stays passing.** Check it on Python 3.13, which is what
  Read the Docs uses (`.readthedocs.yaml`), not only on whatever is to hand: an
  invalid escape sequence such as `"$M_\odot$"` is a `DeprecationWarning` on
  3.11, and so silently ignored, but a `SyntaxWarning` on 3.12+, which the
  ipython directive turns into a build failure. Write LaTeX in labels as a raw
  string — `r"$M_\odot$"` — as the existing figures do.
- **Every `tangos …` command shown in the docs is CI-exercised in *form***: its
  subcommand, and every flag it uses, appear somewhere in `build.sh`. Enforced
  from stage 3 onward by `tests/test_docs_commands.py`, which also requires every
  documented command to parse. A command that genuinely cannot run in
  `build.sh` goes in its `ALLOWLIST`, with a reason. The test compares command *shapes*, not argument values.
  A page is free to choose a different threshold, property list or simulation name
  from whatever CI happens to build with, and `tutorials/first_database` does:
  it filters on `--include-only="NDM()>1000"` where `build.sh` uses `5000`, which is
  far too conservative to make a good tutorial. What the invariant buys is that no
  page can go on showing a subcommand or flag that has been renamed or removed.
- **An unmarked exception in any `.. ipython::` block fails the build**, from stage 3
  onward, and under `-W` so does an unmarked warning. This is the entire difference between examples that are executed and
  examples that are *verified*.

## Style conventions

Second person, present tense. Drop the `_tangos_` italics for pynbody's
``literal``/plain convention. If code can execute it must be `.. ipython::`, never a
hand-typed `# -> 42.0`. One standard prerequisites `.. note::` modelled on
`pynbody/docs/tutorials/quickstart.rst:30-37`, replacing the six verbatim copies of
"Make sure you have followed the initial set up instructions". `:ref:` labels on every
page; `:func:`/`:class:`/`:meth:` roles throughout, so the reference earns its keep;
`.. seealso::` closing each tutorial; `.. versionadded::` for the inline "since
version 1.8.0" notes. Lowercase-underscore filenames — no `+`, no spaces. A
`.. Last checked by <initials>: <date>` line at the top of every file, as in
`pynbody/docs/tutorials/tutorials.rst:1`. One canonical halo-path spelling, explained
once.

## Definition of done

`sphinx-build -W` is clean; an unmarked exception in any ipython block fails the
build; every `tangos …` command shown is CI-exercised in form — its subcommand and
flags appear in `build.sh` — enforced by a test;
every deleted URL redirects; the rebuild banner is gone; and a newcomer gets from
`pip install` to a plotted merger history in under 20 minutes without touching raw
simulation data.
