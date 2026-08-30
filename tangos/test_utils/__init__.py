"""Utilities for fetching the tutorial database used by the documentation

The tutorial database is a ready-made tangos database describing five small
simulations. The documentation build queries it: every ``.. ipython::`` block in
the tutorials runs against it, so the numbers and figures on those pages are
produced by the build rather than transcribed. It is also the quickest way for a
reader to get a database without building one from raw simulation data.

WARNING: This module must not depend on a working tangos installation. Read the
Docs calls it from a ``pre_build`` job, potentially before tangos is importable,
and it deliberately duplicates the small amount of :mod:`tangos.config` logic it
needs rather than importing it. Only use standard library modules here.

This is the direct analogue of ``pynbody.test_utils.precache_test_data``.
"""

import os
import pathlib
import shutil
import urllib.request

# The tutorial database. This is currently a development URL; publishing the
# database, and the much larger files needed to recreate it, at a permanent
# public location is tracked in DOCS_PLAN.md.
TUTORIAL_DATABASE_URL = "https://pub-d85a828023a1452bbd3a294bc72003f0.r2.dev/data.db"

# A tangos database that does not exist is silently created, empty, by anything
# that opens a session (see KNOWN_ISSUES.md), and an empty one is around 80 KB.
# Treat anything implausibly small as absent so that a stray empty file, or a
# download interrupted before this module started using a temporary name, is
# replaced rather than being mistaken for the real thing.
_MINIMUM_PLAUSIBLE_SIZE = 100 * 1024 * 1024


def tutorial_database_path():
    """Return the path the tutorial database should be downloaded to.

    This mirrors how :mod:`tangos.config` resolves the database location --
    ``TANGOS_DB_CONNECTION`` if set, otherwise ``tangos_data.db`` in the user's
    home directory -- without importing tangos. A ``TANGOS_DB_CONNECTION`` that
    names a database server rather than a file has no path to download to, and
    raises :class:`ValueError`.
    """
    db = os.environ.get("TANGOS_DB_CONNECTION")
    if db is None:
        return pathlib.Path.home() / "tangos_data.db"
    if "//" in db:
        raise ValueError(
            f"TANGOS_DB_CONNECTION is set to {db!r}, which names a database server "
            "rather than a file, so the tutorial database cannot be downloaded to it. "
            "Unset it, or point it at a path, to fetch the tutorial database."
        )
    return pathlib.Path(db)


def precache_tutorial_database(path=None, verbose=True):
    """Download the tutorial database, unless it is already present.

    Equivalent to running::

        curl -o ~/tangos_data.db <TUTORIAL_DATABASE_URL>

    Parameters
    ----------
    path : str or pathlib.Path, optional
        Where to put the database. Defaults to :func:`tutorial_database_path`.
    verbose : bool
        Whether to report what is happening. True by default, because this
        downloads well over a gigabyte and silence would be unhelpful.

    Returns
    -------
    pathlib.Path
        The path to the database, whether it was just downloaded or already
        present.
    """
    path = pathlib.Path(path) if path is not None else tutorial_database_path()

    if path.exists() and path.stat().st_size >= _MINIMUM_PLAUSIBLE_SIZE:
        if verbose:
            print(f"Tutorial database already present at {path}")
        return path

    if path.exists():
        # Too small to be the real database; see _MINIMUM_PLAUSIBLE_SIZE.
        if verbose:
            print(f"Replacing implausibly small file at {path} "
                  f"({path.stat().st_size} bytes)")
        path.unlink()

    path.parent.mkdir(parents=True, exist_ok=True)

    # Download to a temporary name and move it into place only once complete, so
    # that an interrupted download cannot leave behind a partial file that later
    # runs would mistake for a usable database.
    partial_path = path.with_name(path.name + ".part")

    if verbose:
        print(f"Downloading tutorial database to {path}")

    # An explicit User-Agent is required, not cosmetic. Cloudflare serves the
    # r2.dev endpoint with bot protection that rejects urllib's default
    # "Python-urllib/x.y" with HTTP 403, while accepting any other value --
    # which is why this works from curl but not from a bare urlopen(). Verified
    # by probing the endpoint with several agents.
    request = urllib.request.Request(
        TUTORIAL_DATABASE_URL,
        headers={"User-Agent": "tangos test_utils (documentation build)"},
    )

    try:
        with urllib.request.urlopen(request) as response, \
                open(partial_path, "wb") as target:
            shutil.copyfileobj(response, target)
        partial_path.replace(path)
    finally:
        if partial_path.exists():
            partial_path.unlink()

    if verbose:
        print(f"Tutorial database ready at {path} ({path.stat().st_size} bytes)")

    return path


if __name__ == "__main__":
    precache_tutorial_database()
