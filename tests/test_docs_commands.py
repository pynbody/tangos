"""Every ``tangos`` command shown in the documentation is exercised, in form, by CI.

This enforces the invariant recorded under "Invariants" in ``DOCS_PLAN.md``: for every
``tangos ...`` command in a code block of a documentation page, its subcommand and every
flag it uses must appear somewhere in ``test_tutorial_build/build.sh``, which the
integration test runs. Only the *shape* of a command is compared -- the subcommand and
the set of flags -- never the argument values, because pages deliberately choose values
different from CI's (``tutorials/first_database`` filters on
``--include-only="NDM()>1000"`` where ``build.sh`` uses 5000).

Commands are parsed with tangos' real argument parser, not by pattern matching, so
``--for``/``--sims``, ``--type=x``/``--type x`` and abbreviated flags all compare equal,
and a documented flag that has been renamed or removed fails to parse at all.

A documented subcommand or flag that genuinely cannot be exercised by ``build.sh`` goes
in ``ALLOWLIST``, with a reason. Allowlisted commands must still parse, and an entry
that no documented command needs any more fails the test, so the list cannot rot.

Needs no database, no network and no simulation data.
"""

import argparse
import contextlib
import functools
import io
import os
import re
import shlex
from dataclasses import dataclass

import tangos.scripts

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
BUILD_SH = os.path.join(REPO_ROOT, "test_tutorial_build", "build.sh")

# Documented commands, or flags of them, that build.sh does not run, keyed by
# (subcommand, flag) where flag is the argparse dest, or None to exempt the whole
# subcommand. Every entry needs a reason.
ALLOWLIST = {
    ("serve", None):
        "starts a web server that runs until killed, so cannot be part of a batch build",
    ("recent-runs", None):
        "read-only inspection of the database's run history; builds nothing",
    ("import-ahf-trees", None):
        "Gadget+AHF merger-tree route; build.sh has no Gadget+AHF build (see DOCS_PLAN.md stage 2)",
    ("import-subfind-trees", None):
        "Gadget-4+SubFind merger-tree route; build.sh builds no Gadget-4 tree (see KNOWN_ISSUES.md)",
    ("add", "max_objects"):
        "how-to/eagle caps a full EAGLE volume; the CI datasets are small enough not to need it",
    ("import-changa-bh", "backend"):
        "import-changa-bh is parallel-aware, but build.sh runs it serially",
    ("rollback", None):
        "deletes earlier runs; a build has nothing to undo",
    ("delete-properties", None):
        "deletes stored properties; a build has nothing to delete (tests/test_property_deleter.py)",
    ("remove-duplicates", None):
        "cleans up after write --force; build.sh never writes a property twice",
    ("thin-timesteps", None):
        "deletes timesteps, which would change the database tangos diff checks (tests/test_timestep_thinner.py)",
    ("import", None):
        "copies one database into another; build.sh builds a single database (tests/test_import.py)",
    ("diff", None):
        "the integration workflow runs it on build.sh's output, rather than build.sh itself",
    ("write", "pickle_results"):
        "an alternative route to the same database; tests/test_pickled_results_writer.py covers it",
    ("write-pickled-results", None):
        "commits the output of write --pickle-results, which build.sh does not use",
}

# Code-block languages whose lines may be shell commands; "" is a block with none given.
SHELL_LANGUAGES = {"", "bash", "sh", "shell", "console", "shell-session", "none", "text",
                   "csh", "tcsh", "zsh"}

# Programs that launch tangos rather than being it, as in ``mpirun -np 4 tangos ...``.
# A shell variable in that position (build.sh's ``$MPI``) is treated the same way.
LAUNCHERS = {"mpirun", "mpiexec", "srun"}

# A word that is a placeholder for the reader to fill in, such as ``<N>`` or
# ``[normal tangos command here]``; a line containing one is not a real command.
PLACEHOLDER = re.compile(r"^\[|\]$|<[A-Za-z_]+>")

ENVIRONMENT_ASSIGNMENT = re.compile(r"^[A-Za-z_]\w*=")

# Destinations that are not worth comparing against build.sh.
IGNORED_DESTS = {"help"}


@dataclass(frozen=True)
class Command:
    location: str  # path relative to the repository root, and line number
    text: str      # the command line, with continuations joined and any prompt removed

@dataclass(frozen=True)
class CommandShape:
    subcommand: str
    flags: frozenset  # argparse dests of the options present on the command line

class CommandParseError(Exception):
    pass


# --- parsing commands with tangos' own parser ----------------------------------------

class _ParseRecorder:
    """Records what the parser actually matched on a command line, as it matches it."""
    def __init__(self):
        self.reset()

    def reset(self):
        self.subcommand = None
        self.flags = set()

_recorder = _ParseRecorder()

@functools.lru_cache(maxsize=None)
def _recording_action_class(action_class):
    class RecordingAction(action_class):
        def __call__(self, parser, namespace, values, option_string=None):
            if isinstance(self, argparse._SubParsersAction):
                _recorder.subcommand = values[0]
            elif option_string is not None:
                _recorder.flags.add(self.dest)
            return super().__call__(parser, namespace, values, option_string)
    return RecordingAction

def _raise_parse_error(message):
    raise CommandParseError(message)

def _instrument(parser, seen):
    if id(parser) in seen:
        return
    seen.add(id(parser))
    parser.error = _raise_parse_error
    for action in parser._actions:
        action.__class__ = _recording_action_class(type(action))
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                _instrument(subparser, seen)

@functools.lru_cache(maxsize=None)
def _parser():
    parser = tangos.scripts.get_argument_parser()
    _instrument(parser, set())
    return parser

def _subparser(subcommand):
    for action in _parser()._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices[subcommand]

def _strip_backend(argv):
    """Remove ``--backend``, as tangos.parallel_tasks does before argparse sees the command line.

    It may appear before or after the subcommand, so it is not part of any subparser."""
    for i, arg in enumerate(argv):
        if arg == "--backend":
            if i + 1 == len(argv):
                raise CommandParseError("--backend needs a value")
            return argv[:i] + argv[i + 2:], True
        if arg.startswith("--backend="):
            return argv[:i] + argv[i + 1:], True
    return argv, False

def _tangos_arguments(words):
    """Return the arguments to ``tangos`` if ``words`` run it, or None if they run something else."""
    words = list(words)
    while words and ENVIRONMENT_ASSIGNMENT.match(words[0]):
        words.pop(0)
    if words and (words[0] in LAUNCHERS or words[0].startswith("$")):
        if "tangos" not in words:
            return None
        words = words[words.index("tangos"):]
    if not words or words[0] != "tangos":
        return None
    return words[1:]

def parse_shape(text):
    """Parse a ``tangos`` command line with the real parser and return its shape."""
    try:
        words = shlex.split(text, comments=True)
    except ValueError as e:
        raise CommandParseError(f"not a valid shell command line ({e})") from e
    argv, backend_given = _strip_backend(_tangos_arguments(words))

    _recorder.reset()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        try:
            _parser().parse_args(argv)
        except SystemExit as e:
            # only --help gets here, since parse errors raise CommandParseError
            if e.code:
                raise CommandParseError(f"parser exited with status {e.code}") from e

    flags = set(_recorder.flags) - IGNORED_DESTS
    if backend_given:
        flags.add("backend")
    return CommandShape(_recorder.subcommand, frozenset(flags))

def _describe_flag(subcommand, dest):
    if dest == "backend":
        return "--backend"
    for parser in (_subparser(subcommand), _parser()):
        for action in parser._actions:
            if action.dest == dest and action.option_strings:
                return "/".join(action.option_strings)
    return dest


# --- finding commands in the documentation -------------------------------------------

_RST_CODE_DIRECTIVE = re.compile(r"^(\s*)\.\.\s+(?:code-block|code|sourcecode)::\s*(\S*)")
_RST_DIRECTIVE = re.compile(r"^\s*\.\.\s")
_MARKDOWN_FENCE = re.compile(r"^\s*(`{3,}|~{3,})\s*(.*?)\s*$")
_MYST_DIRECTIVE = re.compile(r"^\{([\w-]+)\}\s*(\S*)")

def _indentation(line):
    return len(line) - len(line.lstrip())

def _rst_literal_blocks(numbered_lines):
    """Yield the shell-like literal blocks of rst source, as lists of (line number, line)."""
    i = 0
    while i < len(numbered_lines):
        line = numbered_lines[i][1]
        directive = _RST_CODE_DIRECTIVE.match(line)
        if directive:
            is_shell = directive.group(2).lower() in SHELL_LANGUAGES
            indentation = len(directive.group(1))
        elif line.rstrip().endswith("::") and not _RST_DIRECTIVE.match(line):
            is_shell = True  # a paragraph ending in "::" introduces a literal block
            indentation = _indentation(line)
        else:
            i += 1
            continue
        i += 1
        block = []
        while i < len(numbered_lines) and (not numbered_lines[i][1].strip()
                                           or _indentation(numbered_lines[i][1]) > indentation):
            block.append(numbered_lines[i])
            i += 1
        if is_shell:
            yield block

def _markdown_fenced_blocks(numbered_lines):
    """Yield the shell-like fenced blocks of markdown/MyST source, as lists of (line number, line)."""
    i = 0
    while i < len(numbered_lines):
        opening = _MARKDOWN_FENCE.match(numbered_lines[i][1])
        i += 1
        if not opening:
            continue
        fence, info = opening.groups()
        block = []
        while i < len(numbered_lines):
            closing = _MARKDOWN_FENCE.match(numbered_lines[i][1])
            i += 1
            if closing and closing.group(1)[0] == fence[0] and len(closing.group(1)) >= len(fence) \
                    and not closing.group(2):
                break
            block.append(numbered_lines[i - 1])

        directive = _MYST_DIRECTIVE.match(info)
        if directive is None:
            language = info.split()[0] if info else ""
        elif directive.group(1) in ("code-block", "code", "sourcecode"):
            language = directive.group(2)
        elif directive.group(1) == "eval-rst":
            yield from _rst_literal_blocks(block)
            continue
        else:
            continue  # some other MyST directive, e.g. a toctree
        if language.lower() in SHELL_LANGUAGES:
            yield block

def _logical_lines(block):
    """Join backslash continuations, yielding (number of first line, stripped text)."""
    start, parts = None, []
    for number, line in block:
        stripped = line.strip()
        if start is None:
            start = number
        if stripped.endswith("\\"):
            parts.append(stripped[:-1].strip())
            continue
        parts.append(stripped)
        yield start, " ".join(parts)
        start, parts = None, []
    if parts:
        yield start, " ".join(parts)

def _documentation_pages():
    excluded = {"_build", "_layouts", "_sass", "_static", "_templates"}  # not sources
    for directory, subdirectories, filenames in os.walk(DOCS_DIR):
        subdirectories[:] = sorted(d for d in subdirectories
                                   if d not in excluded and not d.startswith("."))
        for filename in sorted(filenames):
            if filename.endswith((".rst", ".md")):
                yield os.path.join(directory, filename)

@functools.lru_cache(maxsize=None)
def documented_commands():
    commands = []
    for page in _documentation_pages():
        with open(page) as f:
            numbered_lines = list(enumerate(f.read().splitlines(), start=1))
        blocks = _rst_literal_blocks if page.endswith(".rst") else _markdown_fenced_blocks
        for block in blocks(numbered_lines):
            for number, line in _logical_lines(block):
                if line.startswith("$ "):
                    line = line[2:].strip()
                words = line.split()
                if _tangos_arguments(words) is None or any(PLACEHOLDER.search(w) for w in words):
                    continue
                commands.append(Command(f"{os.path.relpath(page, REPO_ROOT)}:{number}", line))
    return tuple(commands)


# --- finding commands in build.sh -----------------------------------------------------

def _build_sh_mpi_environments(text):
    """Return the variables detect_mpi exports, once for each of its two branches."""
    function = re.search(r"^detect_mpi\(\)\s*\{\n(.*?)^\}", text, re.MULTILINE | re.DOTALL)
    assert function, "build.sh no longer defines detect_mpi(); update this test"
    branches = re.split(r"^\s*else\b", function.group(1), flags=re.MULTILINE)
    assert len(branches) == 2, "detect_mpi() in build.sh no longer has two branches; update this test"
    exports = [dict(re.findall(r'export\s+(\w+)="([^"]*)"', branch)) for branch in branches]
    names = set().union(*exports)
    return [{name: branch.get(name, "") for name in names} for branch in exports]

@functools.lru_cache(maxsize=None)
def build_sh_commands():
    with open(BUILD_SH) as f:
        text = f.read()
    numbered_lines = list(enumerate(text.splitlines(), start=1))
    location = os.path.relpath(BUILD_SH, REPO_ROOT)
    commands = []
    for environment in _build_sh_mpi_environments(text):
        for number, line in _logical_lines(numbered_lines):
            line = line.replace("$1", "ARG1")  # build.sh's functions take one argument
            for name in sorted(environment, key=len, reverse=True):  # $MPIBACKEND before $MPI
                line = line.replace("${%s}" % name, environment[name])
                line = line.replace("$" + name, environment[name])
            command = Command(f"{location}:{number}", line.strip())
            if _tangos_arguments(line.split()) is not None and command not in commands:
                commands.append(command)
    return tuple(commands)

@functools.lru_cache(maxsize=None)
def exercised_by_build_sh():
    """Every (subcommand, None) and (subcommand, flag) that some build.sh command uses."""
    exercised = set()
    for command in build_sh_commands():
        shape = parse_shape(command.text)
        exercised.add((shape.subcommand, None))
        exercised.update((shape.subcommand, flag) for flag in shape.flags)
    return frozenset(exercised)

@functools.lru_cache(maxsize=None)
def documented_shapes():
    """Map each documented command that parses to its shape; failures are test 1's business."""
    shapes = {}
    for command in documented_commands():
        try:
            shapes[command] = parse_shape(command.text)
        except CommandParseError:
            pass
    return shapes

def _requirements(shape):
    yield shape.subcommand, None
    for flag in sorted(shape.flags):
        yield shape.subcommand, flag

def _allowlist_entry_covering(requirement):
    subcommand, _ = requirement
    for key in ((subcommand, None), requirement):
        if key in ALLOWLIST:
            return key
    return None


# --- the tests ------------------------------------------------------------------------

def test_commands_are_found():
    # guards against the extraction silently finding nothing, which would pass everything
    assert len(documented_commands()) > 30
    assert len(build_sh_commands()) > 30
    subcommands = {shape.subcommand for shape in documented_shapes().values()}
    assert {"add", "import-properties", "link", "write"} <= subcommands

def test_build_sh_commands_parse():
    failures = []
    for command in build_sh_commands():
        try:
            parse_shape(command.text)
        except CommandParseError as e:
            failures.append(f"{command.location}: `{command.text}`: {e}")
    assert not failures, "build.sh commands that tangos cannot parse:\n" + "\n".join(failures)

def test_documented_commands_parse():
    failures = []
    for command in documented_commands():
        try:
            parse_shape(command.text)
        except CommandParseError as e:
            failures.append(f"{command.location}: `{command.text}`: {e}")
    assert not failures, ("Documented tangos commands that tangos cannot parse -- a renamed or "
                          "removed subcommand or flag, or a typo:\n" + "\n".join(failures))

def test_documented_commands_are_exercised_by_build_sh():
    exercised = exercised_by_build_sh()
    failures = []
    for command, shape in documented_shapes().items():
        for requirement in _requirements(shape):
            if requirement in exercised or _allowlist_entry_covering(requirement):
                continue
            subcommand, flag = requirement
            if flag is None:
                what = f"subcommand `{subcommand}` is never run by build.sh"
            else:
                what = (f"`{subcommand}` is never run with {_describe_flag(subcommand, flag)} "
                        f"by build.sh")
            failures.append(f"{command.location}: `{command.text}`: {what} "
                            f"[allowlist key {requirement!r}]")
    assert not failures, (
        "Documented tangos commands whose form is not exercised by test_tutorial_build/build.sh. "
        "Either make build.sh exercise it, or, if it cannot, add the key to ALLOWLIST in "
        "tests/test_docs_commands.py with a reason:\n" + "\n".join(failures))

def test_allowlist_is_not_stale():
    exercised = exercised_by_build_sh()
    needed = set()
    for shape in documented_shapes().values():
        for requirement in _requirements(shape):
            if requirement not in exercised:
                needed.add(_allowlist_entry_covering(requirement))
    stale = [f"{key!r}: {reason}" for key, reason in ALLOWLIST.items() if key not in needed]
    assert not stale, ("ALLOWLIST entries in tests/test_docs_commands.py that no documented "
                       "command needs -- the docs no longer use them, build.sh now exercises "
                       "them, or a whole-subcommand entry already covers them. Remove:\n"
                       + "\n".join(stale))
