#
# tangos documentation build configuration file.
#
# Modelled closely on pynbody's docs/conf.py (see
# https://github.com/pynbody/pynbody/blob/master/docs/conf.py) so that the two
# projects' documentation share a look and feel. See the tangos docs README /
# the accompanying report for a list of what was deliberately dropped relative
# to the pynbody version (the IPython `.. ipython::` exception-hooking, the
# matplotlib `.. plot::` directive support, and inheritance diagrams) because
# nothing in tangos' docs currently uses them.

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
              ]

# tangos' existing tutorial pages are plain markdown (ported from the old
# Jekyll site) rather than reST, so both suffixes are source files. myst_parser
# renders the .md files; nbsphinx renders the one pre-executed .ipynb tutorial.
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

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

import tangos

# tangos' ORM relationships are attached via SQLAlchemy `backref`, which is not
# materialised on the class until the mappers are configured (normally the
# first time a session is used). The curated API reference documents several
# of these backref-only attributes explicitly by name (e.g. Simulation.timesteps,
# SimulationObjectBase.links) -- autodoc inspects the class directly, without
# ever opening a database, so without this call those attributes are invisible
# to it and autodoc emits "missing attribute mentioned in :members: option".
from sqlalchemy.orm import configure_mappers

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
