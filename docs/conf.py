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
              'nbsphinx',
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

# ipython_warning_is_error is kept at False (rather than its usual default of True):
# that flag makes *both* unexpected exceptions and unexpected python warnings raised
# inside `.. ipython::` blocks fatal, and we only want the former (regressions that leave
# a traceback in the rendered docs), not the latter (e.g. informational/deprecation
# warnings, which are routine and shouldn't break the readthedocs build). So instead we
# hook the specific log message that IPython.sphinxext.ipython_directive emits for an
# unmarked exception (one raised inside a block that isn't marked with :okexcept:) and
# turn just that into a build failure.
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
# renders the .md files; nbsphinx renders the one pre-executed .ipynb tutorial.
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

nbsphinx_input_prompt = 'In [%s]:'
nbsphinx_output_prompt = 'Out[%s]:'
nbsphinx_execute = 'never'  # the notebook is expensive to evaluate, so we need it pre-evaluated

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

version = ".".join(tangos.__version__.split(".")[:2])
release = tangos.__version__

exclude_patterns = ['_build', '**.ipynb_checkpoints']

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
