"""The example code that documentation pages include from ``docs/examples`` really works.

The documentation build has the tutorial database but no simulation data, so a page
that asks the reader to write a handler or a property and run ``tangos write`` with
it cannot execute that step itself. Instead, the page includes the example from a
file under ``docs/examples`` and this test runs the same commands against it, checking
the results the page quotes. See "Write-side examples are verified outside the docs
build" under Stage 4 in ``DOCS_PLAN.md``.

Each test runs the documented ``tangos`` commands in a subprocess with its own sqlite
database and simulation folder, so it is independent of the database backend the rest
of the test suite is configured for.
"""

import os
import subprocess
import sys

import pytest

EXAMPLES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "examples"))
TEST_SIMULATIONS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "test_simulations"))


@pytest.fixture
def run_in_scratch_database(tmp_path):
    simulation_folder = tmp_path / "simulations"
    simulation_folder.mkdir()
    environment = dict(os.environ,
                       TANGOS_DB_CONNECTION=str(tmp_path / "data.db"),
                       TANGOS_SIMULATION_FOLDER=str(simulation_folder),
                       PYTHONPATH=os.pathsep.join(p for p in (EXAMPLES_DIR, os.environ.get("PYTHONPATH"))
                                                  if p))
    # tangos ignores TANGOS_PROPERTY_MODULES while pytest is running, which it detects
    # from this variable; the commands under test are not part of the test suite
    environment.pop("PYTEST_CURRENT_TEST", None)

    def run(*args, **extra_environment):
        result = subprocess.run(args, env=dict(environment, **extra_environment), cwd=tmp_path,
                                capture_output=True, text=True)
        assert result.returncode == 0, f"{' '.join(args)} failed:\n{result.stdout}\n{result.stderr}"
        return result.stdout

    run.simulation_folder = simulation_folder
    return run

def python(code):
    return sys.executable, "-c", code


def test_custom_handler(run_in_scratch_database):
    """docs/how-to/custom_handlers.rst, with docs/examples/myhandler.py"""
    run = run_in_scratch_database
    run("tangos", "add", "test_my_handler", "--handler=myhandler.MyHandler")
    run("tangos", "link", "--for", "test_my_handler")

    output = run(*python(
        "import tangos\n"
        "for ts in tangos.get_simulation('test_my_handler').timesteps:\n"
        "    print(ts.extension, ts.halos.count())\n"
        "halo = tangos.get_object('test_my_handler/my_timestep_2/halo_10')\n"
        "print(halo.previous)\n"
        "print(halo.previous.load())\n"
        "print(tangos.get_object('test_my_handler/my_timestep_2/halo_120').previous)\n"))
    lines = output.splitlines()
    assert lines[0] == "my_timestep_1 100"
    assert lines[1] == "my_timestep_2 150"
    assert lines[2].startswith("<Halo 'test_my_handler/my_timestep_1/halo_10' |")
    expected_data = os.path.join(str(run.simulation_folder), "test_my_handler", "my_timestep_1")
    assert lines[3] == f"Data for {expected_data} halo 10"
    assert lines[4] == "None"  # halos 101-150 exist only in the second timestep

    run("tangos", "write", "my_fantastic_property", "--for", "test_my_handler")
    output = run(*python(
        "import tangos\n"
        "print(tangos.get_object('test_my_handler/my_timestep_1/halo_5')['my_fantastic_property'])\n"
        "print(len(tangos.get_timestep('test_my_handler/my_timestep_2')"
        ".calculate_all('my_fantastic_property')[0]))\n"))
    assert output.splitlines() == ["42.0", "150"]


def test_properties_from_particles(run_in_scratch_database):
    """docs/tutorials/custom_properties.rst and histograms.rst, with mytangosproperty.py and myhistogram.py

    The repository's small test simulation has no halos whose particles tangos write
    can load (see KNOWN_ISSUES.md), so the properties are written for a tracker of
    all its star particles instead; tangos hands them particles in the same way."""
    run = run_in_scratch_database
    run("tangos", "add", "test_tipsy", TANGOS_SIMULATION_FOLDER=TEST_SIMULATIONS_DIR)
    run(*python(
        "import tangos\n"
        "from tangos import tracking\n"
        "tracking.new('test_tipsy', tangos.get_timestep('test_tipsy/%832').load().star)\n"),
        TANGOS_SIMULATION_FOLDER=TEST_SIMULATIONS_DIR)

    run("tangos", "write", "velocity_dispersion", "metals_into_stars_histogram",
        "--type", "tracker", "--for", "test_tipsy",
        TANGOS_SIMULATION_FOLDER=TEST_SIMULATIONS_DIR, TANGOS_PROPERTY_MODULES="mytangosproperty,myhistogram")
    output = run(*python(
        "import tangos\n"
        "tracker = tangos.get_object('test_tipsy/%832/tracker_1')\n"
        "print(tracker['velocity_dispersion'])\n"
        "history = tracker['metals_into_stars_histogram']\n"
        "print(len(history), history.sum())\n"),
        TANGOS_SIMULATION_FOLDER=TEST_SIMULATIONS_DIR, TANGOS_PROPERTY_MODULES="mytangosproperty,myhistogram")
    velocity_dispersion, history = output.splitlines()
    assert 0 < float(velocity_dispersion) < 1000  # km/s
    history_length, history_total = history.split()
    assert int(history_length) == 141  # 20 Myr bins up to the snapshot's 2.82 Gyr
    assert float(history_total) > 0
