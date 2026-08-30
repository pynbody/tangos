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
pip install -e .[docs]      # pandoc must also be installed, for nbsphinx
cd docs && python -m sphinx -W -b html . _build/html
```

It takes a few minutes. `-W` matches CI: the build is warning-free and must stay so.

**Run `pre-commit run --all-files` before you push**, and commit anything it changes.
The hooks run in CI and will fail the build otherwise. isort in particular reorders
imports in `docs/conf.py`, which is easy to miss because nothing in the documentation
build itself complains.

## Status

Stage 0 is complete. Stages 1–5 have not started.

## Stages

Effort estimates are person-days. **Stages 1–2 alone ship a coherent site**; the rest
deepens it.

### Stage 0 — build system (complete)

What now exists, as context for later stages:

- Sphinx with `sphinx_book_theme`, configured in `docs/conf.py` closely following
  pynbody's. `myst_parser` renders the existing `.md` pages unchanged; `nbsphinx`
  renders the notebook. The page structure is still the old one, ported as-is.
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

### Stage 1 — the newcomer path (5 d)

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

- [ ] `explanation/concepts` (new, ~150 lines) — the data model. Highest
      value-per-line item in the whole plan
- [ ] `index` — rewrite as a real landing page
- [ ] `installation`
- [ ] `configuration` (new) — `config_local.py` is documented in `tangos/config.py`
      and nowhere in `docs/`
- [ ] `tutorials/quickstart` — from notebook cells 0–16
- [ ] `tutorials/time_series` — from notebook cells 17–19, 28–32
- [ ] `tutorials/live_calculations` — from `live_calculation.md:1-346`
- [ ] `tutorials/webserver` — grow the 21-line stub
- [ ] Start the redirect map (see **Invariants**). URLs first move here; stage 2
      moves many more and continues the same map

These should be written as `ipython` blocks as far as possible. For development
purposes, the sample database is available at https://pub-d85a828023a1452bbd3a294bc72003f0.r2.dev/data.db
This should enable successful recalculation of all the existing notebook cells; if not,
pause and ask for advice.


### Stage 2 — collapse the first_steps combinatorics (4 d)

The six `first_steps_*` pages are ~70% byte-identical — the `--with-prerequisites`
bullet list is word-for-word across subfind, rockstar and ramses. Only eight things
vary, and only **two** branch the command *sequence*: whether the finder supplies its
own merger trees, and whether the format has a group/halo hierarchy.

- [ ] `tutorials/first_database` — one canonical page on **ChaNGa+AHF**: the only
      dataset with baryons, so `uvi_image`/`SFR_histogram` work and everything
      downstream depends on them, and it has the black-hole companion run
- [ ] `sphinx-design` `tab-set` with `:sync:` keys at exactly two steps — "import
      finder properties" and "build the merger tree" — so choosing a finder once
      reconfigures the page. Every other step stays single-path prose
- [ ] `reference/simulation_formats` — all combinations as flat rows, modelled on
      `pynbody/docs/loaders.rst:10-22`
- [ ] Keep EAGLE and yt as separate short pages; their differences are not in the
      command sequence
- [ ] Retire `old.md`, `advanced.md`, `first_steps.md`, `data_exploration.md`,
      `bluewaters.md`, `troubleshooting.md`. Leave the Jekyll files until the Pages
      question in **Before merging to master** is settled
- [ ] Redirects for every deleted URL

The tradeoff of tabs is real: they defeat Ctrl-F and PDF output, and deep-linking
gets worse. That is why they are restricted to two steps and backed by a searchable
flat table.

### Stage 3 — make the examples executable and verified (3 d)

tangos cannot do directly what pynbody does. pynbody's build-time dependency is a
snapshot file it reads itself; tangos needs a *populated database*, meaning pynbody +
yt + particle data + hours of `tangos write` (`build.sh` says 35 GB). But that
database is **already built on every PR**:
`.github/workflows/integration-test.yaml` runs `INTEGRATION_TESTING=1 bash build.sh`
against mini data (Zenodo 12189455), uploads `data.db` as an artifact, and verifies it
with `tangos diff`. So decouple database *production* from docs *building*.

- [ ] **Needs a human first**: publish the tutorial database and the files needed to
      recreate it in a location with better scaling than Zenodo.
- [ ] Add `tangos.test_utils.precache_tutorial_database()`, a direct analogue of
      `pynbody.test_utils.precache_test_data()`. **Highest-leverage new code in the
      plan**: it fixes the RTD build and reproducibility, and replaces four separate
      hand-written Zenodo-URL paragraphs that all point at a 2021 database
- [ ] `pre_build` in `.readthedocs.yaml` downloads only that sqlite file. Budget: ~1
      min pip (no pynbody, no compiler) + ≤2 min download + ~2–4 min for ~100 ipython
      blocks ≈ 5–7 min, well inside RTD's ~30 min cap. Explicitly excluded:
      pynbody/yt installs, particle data, any `tangos write`
- [ ] Convert tutorial code fences to `.. ipython::` / `@savefig`
- [ ] Bind the write-side shell commands to `build.sh` — shared `_snippets/*.sh` via
      `literalinclude`, or a grep test. They are already CI-verified; the docs simply
      do not reuse that fact
- [ ] Re-enable the integration test workflow (see **Before merging to master**) —
      this stage depends on the database it builds, so restore it here rather than at
      the end

Fallback, worth adopting as a floor regardless: `SimulationGeneratorForTests` builds
8 timesteps × 9 halos with links in 1.7 s, with no pynbody and no data. That alone
keeps the language reference executable if a download fails.

### Stage 4 — gap-filling how-tos and remaining tutorials (5 d)

- [ ] `how-to/managing_a_database` (new) — the biggest single gap
- [ ] `how-to/large_simulations` (new)
- [ ] `how-to/parallel`, `how-to/database_backends`
- [ ] `how-to/custom_handlers`, absorbing `using_with_yt.md`
- [ ] Port `custom_properties`, `histograms`, `black_holes`, `crossmatching`, `tracking`
- [ ] Web interface, currently a 21-line stub. Undocumented: arbitrary
      live-calculation expressions as timestep-view columns (arguably the killer
      feature), the merger-tree viewer, and the entire URL API in
      `tangos/web/routes.py` — `.../{x}/vs/{y}.csv`,
      `.../gather/{typetag}/{name}.json`, `cascade_plot`
- [ ] The four supported code × finder combinations with no page at all:
      Gadget-4+SubFind, Gadget-4+HBT+, Ramses+AdaptaHOP, Enzo+Rockstar-via-yt. Two are
      built by `build.sh` on every PR
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
      `reference/simulation_formats`, `configuration`, `explanation/concepts`. Do each
      as the page it names lands
- [ ] FAQ, link audit

## Target architecture

**Setup** — `index` · `installation` · `configuration`

**Tutorials**, linear and in this order. The first five need no simulation data and
no pynbody: `explanation/concepts` → `tutorials/quickstart` → `tutorials/time_series`
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
- **`pandoc` is a build dependency**, not optional: nbsphinx shells out to it.
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
- [ ] **Re-enable the integration test workflow**: uncomment `pull_request:` in
      `.github/workflows/integration-test.yaml`. It was disabled for the rebuild
      because it builds a test database from real simulation data on every PR, which
      no docs change warrants. Stage 3 needs it back earlier than this.
- [ ] **Remove the rebuild-in-progress banner**:
      `html_theme_options["announcement"]` in `docs/conf.py` and the
      `.bd-header-announcement` rules in `docs/_static/custom.css`. It points readers
      at the old site, which merging this work retires.

## Invariants

These span PRs and are easy to break one stage at a time.

- **Every deleted page URL gets a redirect.** The six `first_steps_*.html` URLs are
  linked from the README and probably from published papers.
- **`sphinx-build -W` stays passing.**
- **Every `tangos …` command shown in the docs also appears in `build.sh`**, enforced
  by a test, from stage 3 onward.
- **An unmarked exception in any `.. ipython::` block fails the build**, from stage 3
  onward. This is the entire difference between examples that are executed and
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
build; every `tangos …` command shown also appears in `build.sh`, enforced by a test;
every deleted URL redirects; the rebuild banner is gone; and a newcomer gets from
`pip install` to a plotted merger history in under 20 minutes without touching raw
simulation data.
