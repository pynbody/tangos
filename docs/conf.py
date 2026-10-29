#
# tangos documentation build configuration file.
#
# Modelled closely on pynbody's docs/conf.py (see
# https://github.com/pynbody/pynbody/blob/master/docs/conf.py) so that the two
# projects' documentation share a look and feel. See the tangos docs README /
# the accompanying report for a list of what was deliberately dropped relative
# to the pynbody version (the matplotlib `.. plot::` directive support and
# inheritance diagrams) because nothing in tangos' docs currently uses them.

import os
import sys

sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('..'))
# docs/examples holds example modules that pages include with literalinclude; the
# tutorials also import some of them in their ipython blocks, as a reader would
# import their own (see tests/test_docs_examples.py, which runs the rest).
sys.path.insert(0, os.path.abspath('examples'))

# Force the non-interactive Agg backend before anything else touches
# matplotlib. IPython.sphinxext.ipython_directive normally does this itself
# (via ipython_mplbackend below), but its guard against doing so twice checks
# `'matplotlib.backends' not in sys.modules` -- and on current matplotlib that
# submodule is already imported as a side effect of `import matplotlib`, so
# the guard is always false and the directive silently skips setting the
# backend. Without this, the first figure created during the build resolves
# to the platform's interactive backend (e.g. macosx), which then crashes
# with `NotImplementedError: Implement enable_gui in a subclass` because the
# ipython directive's embedded shell doesn't support a GUI event loop.
import matplotlib

matplotlib.use('agg')

# -- General configuration -----------------------------------------------------

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.doctest',
              'sphinx.ext.todo',
              'sphinx.ext.napoleon',
              'sphinx.ext.coverage',
              'sphinx.ext.mathjax',
              'sphinx.ext.ifconfig',
              'sphinx.ext.viewcode',
              'sphinx_copybutton',
              'numpydoc',
              'myst_parser',
              'sphinx_design',
              'sphinxarg.ext',
              'sphinx_reredirects',
              ]

# Every page URL this rebuild retires gets a redirect, because the old URLs are
# linked from the README, from docstrings inside tangos itself, and probably from
# published papers (see "Invariants" in DOCS_PLAN.md). The map grows with each
# stage; stage 2, which retires six first_steps_* pages at once, is the big one.
#
# Keys are the old document names, without extension; values are paths relative to
# the old page. sphinx-reredirects writes a small HTML file at each old URL.
redirects = {
    # Stage 1: the 21-line stub grew into a full page under tutorials/.
    "data_exploration_webserver": "tutorials/webserver.html",
    # Stage 2: the six first_steps_* pages collapse into one canonical
    # ChaNGa+AHF walkthrough, and the per-combination detail that used to
    # justify a page each now lives as rows in the simulation-formats table.
    # So the two ChaNGa-flavoured URLs (and the old index over all six) go to
    # the tutorial, and the other four go to the table.
    "first_steps": "tutorials/first_database.html",
    "first_steps_changa+ahf": "tutorials/first_database.html",
    "first_steps_gadget+subfind": "reference/simulation_formats.html",
    "first_steps_gadget+rockstar": "reference/simulation_formats.html",
    "first_steps_gadget+AHF": "reference/simulation_formats.html",
    "first_steps_ramses+hop": "reference/simulation_formats.html",
    # EAGLE keeps its own short page, moved under how-to/. yt went to how-to/yt in
    # stage 2 and into how-to/custom_handlers in stage 4; how-to/yt.html was never
    # published, so needs no redirect of its own.
    "first_steps_eagle": "how-to/eagle.html",
    "using_with_yt": "how-to/custom_handlers.html#using-yt-instead-of-pynbody",
    # data_exploration.md was the "explore an existing database" page; that is
    # now the quick-start tutorial.
    "data_exploration": "tutorials/quickstart.html",
    # old.md was the pre-1.0 halo_database README, mostly about building a
    # database from scratch.
    "old": "tutorials/first_database.html",
    # advanced.md was nothing but a list of links to pages that all still
    # exist, so the landing page is the closest thing to its content.
    "advanced": "index.html",
    # Stage 3: the notebook was the original data-exploration tutorial, converted
    # cell by cell into quickstart, time_series and the live calculations tutorial
    # (since folded into quickstart and how-to/live_calculation_recipes).
    # Its name has spaces, which get_target_uri() quotes, so the build-finished
    # hook below also writes this stub at the literal name.
    "Data exploration with python": "tutorials/quickstart.html",
    # Stage 4: the how-to pages move under how-to/. mpi.html is also linked from
    # tangos/core/halo.py, and custom_input_handlers.html (as soon as
    # KNOWN_ISSUES.md's dead input_handlers.html link is fixed) from
    # tangos/input_handlers/__init__.py.
    "mpi": "how-to/parallel.html",
    "rdbms": "how-to/database_backends.html",
    "custom_input_handlers": "how-to/custom_handlers.html",
    # Stage 4b: the "going further" pages become tutorials after first_database.
    # black_holes_and_crossmatching splits in two; its URL goes to the first half.
    # custom_properties.html keeps its page for now -- the reference half of it --
    # so it gets no redirect until stage 5 moves that too.
    "histogram_properties": "tutorials/histograms.html",
    "black_holes_and_crossmatching": "tutorials/black_holes.html",
    "tracking": "tutorials/tracking.html",
}

# ipython_savefig_dir is where `.. ipython::` @savefig figures land; ported
# from pynbody as-is. plot_working_directory is a matplotlib.sphinxext.plot_directive
# setting that pynbody sets alongside it -- that directive itself is not enabled here
# (see the module docstring above), but the value is harmless to carry over and saves
# a surprise if/when stage 3 or later turns .. plot:: on.
ipython_savefig_dir = 'plots'
plot_working_directory = '.'

extensions += ['IPython.sphinxext.ipython_console_highlighting',
               'IPython.sphinxext.ipython_directive']

# ipython_warning_is_error is set to False, which means the ipython directive *reports*
# an unmarked exception (inside a block not marked :okexcept:) or an unmarked python
# warning (not marked :okwarning:) as a Sphinx warning instead of raising. Under -W --
# and so on Read the Docs, with fail_on_warning -- either one fails the build. That is
# wanted: mark a block :okwarning: when its warning is expected.
#
# Without -W (`make html`, whose SPHINXOPTS is empty), both would be let through and the
# build would succeed with a traceback rendered into the page. The filter below makes an
# unmarked exception fatal regardless of -W, with a message saying what to do about it.
#
# Verified in stage 3 with deliberately failing blocks, under -W: unmarked exception ->
# fails (with or without the filter); :okexcept: -> passes; unmarked warning -> fails.
ipython_warning_is_error = False

import logging as _logging


class _FailOnUnexpectedIPythonException(_logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        if "Exception in " in message and "at block ending on line" in message:
            raise RuntimeError(
                "An ipython:: block raised an exception that isn't marked with :okexcept:; "
                "see above for the traceback. If the exception is intentional (e.g. "
                "illustrating an error case), add :okexcept: to the block.\n" + message
            )
        return True


_logging.getLogger('sphinx.IPython.sphinxext.ipython_directive').addFilter(
    _FailOnUnexpectedIPythonException())

# tangos' existing tutorial pages are plain markdown (ported from the old
# Jekyll site) rather than reST, so both suffixes are source files. myst_parser
# renders the .md files.
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# Without this, MyST does not generate an #anchor for any heading, so plain markdown
# links from one page to a heading on another (or within the same page) -- e.g.
# `custom_properties.md#using-the-particle-data-outside-the-halo` -- have nothing to
# land on. Depth 3 covers every heading level currently in use (the pages are at most
# title/H1 + a couple of H2/H3 sections deep).
myst_heading_anchors = 3

# numpydoc's strict docstring validation (numpydoc_validation_checks) is off by
# default and left that way here: tangos' docstrings are known to be sparse and
# uneven at this stage, and that is expected and not something this pass fixes.
# numpydoc_show_class_members is disabled because it adds a "Methods"
# autosummary table to every documented class, which for tangos' largely
# undocumented methods produces more noise than signal.
numpydoc_show_class_members = False

# tangos' own docstrings are inconsistent (a mix of one-liners, plain prose, and
# occasional Sphinx-style :param: field lists rather than numpydoc/Google
# sections). Keep napoleon enabled (as pynbody does) so any numpy/Google-style
# docstrings that do exist are still picked up, but do not chase warnings about
# the many docstrings that have no structured sections at all.
napoleon_google_docstring = True
napoleon_numpy_docstring = True

# A handful of tangos submodules do a genuine runtime import of an optional
# dependency that isn't (and shouldn't need to be) part of the docs build
# environment: most of the pynbody/yt input handlers do their own lazy/deferred
# imports and so import cleanly without these, but
# tangos.parallel_tasks.pynbody_server, the MPI parallel-tasks backends, and
# tangos.scripts.preprocess_bh import their optional dependency at module
# level. Mock those so autosummary can still document the modules that need
# them, rather than installing pynbody/yt/mpi4py just to build the docs.
# pynbody is mocked for autodoc even though it is now a real documentation
# dependency (see setup.py): the tutorials import it at build time, but autodoc
# should not need a working pynbody to read signatures out of tangos' own modules.
autodoc_mock_imports = ['mpi4py', 'pypar', 'Simpy', 'pynbody']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The master toctree document. tangos' homepage is ported as-is from the old
# Jekyll index.md (via myst_parser) rather than rewritten as index.rst, per the
# "port markdown in place, minimal editing" brief -- see index.md for the
# toctree that was appended to it.
master_doc = 'index'

# General information about the project.
project = 'tangos'
copyright = '2018-%Y'
author = 'tangos team'

from sqlalchemy.orm import configure_mappers

import tangos

# tangos' ORM relationships are attached via SQLAlchemy `backref`, which is not
# materialised on the class until the mappers are configured (normally the first
# time a session is used). The curated API reference documents several of these
# backref-only attributes explicitly by name (e.g. Simulation.timesteps,
# SimulationObjectBase.links) -- autodoc inspects the class directly, without ever
# opening a database, so without this call those attributes are invisible to it and
# autodoc emits "missing attribute mentioned in :members: option". The call must
# follow `import tangos`; keep this comment on the call rather than on the import,
# so isort does not separate it from what it explains.
configure_mappers()


# Every `.. ipython::` block in the tutorials queries the tutorial database, but a
# tangos database that does not exist is silently created, empty, by the first query
# (see KNOWN_ISSUES.md). Without this check a build that skipped
# precache_tutorial_database() stays green under -W, and every page renders with
# empty output. So refuse to build unless the database is there and holds what the
# tutorials expect. The check opens the file read-only, via sqlite3 rather than
# tangos, precisely so that it cannot create the empty file it is guarding against.
_TUTORIAL_SIMULATIONS = {"tutorial_changa", "tutorial_changa_blackholes", "tutorial_gadget",
                         "tutorial_gadget_rockstar", "tutorial_ramses"}


def _check_tutorial_database():
    import sqlite3
    from pathlib import Path

    how_to_fix = ("Fetch it with\n\n"
                  "    python -c \"import tangos.test_utils as tu; tu.precache_tutorial_database()\"\n\n"
                  "before building the docs (see DOCS_PLAN.md).")
    db = tangos.config.db
    if "//" in db:
        raise RuntimeError(f"The docs must be built against the tutorial database, which is an sqlite "
                           f"file, but TANGOS_DB_CONNECTION names a database server ({db!r}). " + how_to_fix)
    if not Path(db).is_file():
        raise RuntimeError(f"The tutorial database was not found at {db}. " + how_to_fix)
    try:
        with sqlite3.connect(Path(db).absolute().as_uri() + "?mode=ro", uri=True) as connection:
            present = {name for (name,) in connection.execute("SELECT basename FROM simulations")}
    except sqlite3.Error as e:
        raise RuntimeError(f"{db} is not a readable tangos database ({e}). " + how_to_fix) from e
    missing = _TUTORIAL_SIMULATIONS - present
    if missing:
        raise RuntimeError(f"{db} does not hold the tutorial simulations: missing {sorted(missing)}. "
                           + how_to_fix)


_check_tutorial_database()

version = ".".join(tangos.__version__.split(".")[:2])
release = tangos.__version__

exclude_patterns = ['_build', '_web_screenshots', '**.ipynb_checkpoints']

pygments_style = 'sphinx'

# -- Options for HTML output ---------------------------------------------------

html_theme = 'sphinx_book_theme'

# The "logo" dict adds a wordmark next to the logo image. pynbody does not need
# this because its logo.svg is a wide banner with the word "pynbody" already
# drawn into the artwork; the tangos logo is a tall, square-ish glyph with no
# wordmark, so the name has to come from the theme instead. custom.css then lays
# the two out as a horizontal row -- see the note there.
#
# The announcement is a temporary, site-wide banner for the duration of the
# documentation rebuild. REMOVE IT when docs-refactor-main merges into master
# (tracked in DOCS_PLAN.md).
html_theme_options = {
    "repository_url": "https://github.com/pynbody/tangos",
    "use_repository_button": True,
    "logo": {
        "text": "tangos",
    },
    "announcement": (
        "<strong>This documentation is being rebuilt.</strong> "
        "Pages may be incomplete, out of date, or not work as described. "
        "The previous documentation remains available at "
        "<a href='https://pynbody.github.io/tangos/'>pynbody.github.io/tangos</a>."
    ),
}

html_logo = "_static/logo.svg"

html_static_path = ['_static']
# Quicksand is the tangos web interface's font (see tangos/web/templates/
# layout.jinja2), loaded here from the same Google Fonts URL with the same weights,
# so the docs' wordmark matches the application's.
html_css_files = [
    'https://fonts.googleapis.com/css?family=Quicksand:400,500,700',
    'custom.css',
]

htmlhelp_basename = 'tangosdoc'

copybutton_copy_empty_lines = False
copybutton_selector = "div.highlight > pre"
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.{3,5}: | {5,8}: "
copybutton_prompt_is_regexp = True
copybutton_only_copy_prompt_lines = True


def _write_unquoted_redirect_stubs(app, exception):
    """Also write each redirect stub at the *unquoted* filename.

    Four of the retired pages have a ``+`` in their name (``first_steps_gadget+AHF``
    and friends). sphinx-reredirects writes its stub at
    ``builder.get_target_uri(docname)``, and Sphinx percent-encodes the docname
    there, so the stub lands at ``first_steps_gadget%2BAHF.html`` -- while the URL
    the Jekyll site published, and which the README and any citing paper link to, has
    a literal ``+``. Nothing warns about this: the build succeeds under ``-W`` and the
    redirect simply never fires.

    So for any redirect whose docname is altered by quoting, copy the stub to the
    literal name as well. Both files are left in place, since either spelling may be
    what a given link uses. See "Invariants" in DOCS_PLAN.md.
    """
    if exception is not None or app.builder.name != "html":
        return

    from pathlib import Path
    from urllib.parse import quote

    outdir = Path(app.outdir)
    for docname in redirects:
        if quote(docname) == docname:
            continue
        quoted = outdir / (quote(docname) + ".html")
        literal = outdir / (docname + ".html")
        if quoted.exists() and not literal.exists():
            literal.write_text(quoted.read_text(encoding="utf-8"), encoding="utf-8")


# tutorials/webserver shows the web interface in screenshots taken during the build
# (see web_screenshots.py), and fetches data from it in its ipython blocks, so a
# tangos server runs against the tutorial database for the whole build.
def _start_web_server_and_capture(app):
    import atexit

    import web_screenshots
    try:
        import playwright  # noqa: F401
    except ImportError as e:
        raise RuntimeError("Building the docs takes screenshots of the web interface, which needs "
                           "playwright: pip install -e .[docs], then "
                           "python -m playwright install chromium") from e
    web_screenshots.start_server()
    atexit.register(web_screenshots.stop_server)
    web_screenshots.capture(os.path.join(app.srcdir, "_web_screenshots"))


def _stop_web_server(app, exception):
    import web_screenshots
    web_screenshots.stop_server()


def setup(app):
    app.connect("builder-inited", _start_web_server_and_capture)
    app.connect("build-finished", _stop_web_server)
    app.connect("build-finished", _write_unquoted_redirect_stubs)
