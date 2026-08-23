import glob
import os
import shutil

import pytest
from test_db_writer import (
    _assert_properties_as_expected,
    fresh_database,
    run_writer_with_args,
)

import tangos as db
from tangos.tools import pickled_results_writer


@pytest.fixture
def tangos_results_dir():
    if os.path.exists("tangos_results"):
        shutil.rmtree("tangos_results")
    yield "tangos_results"
    if os.path.exists("tangos_results"):
        shutil.rmtree("tangos_results")


def _commit_pickled_results(files):
    tool = pickled_results_writer.PickledResultsWriter()
    tool.parse_command_line(files)
    tool.run_calculation_loop()


def test_pickled_results_are_committed_and_files_deleted(fresh_database, tangos_results_dir):
    run_writer_with_args("dummy_property", "--pickle-results")

    files = sorted(glob.glob(os.path.join(tangos_results_dir, "*.pickle")))
    assert len(files) > 0

    _commit_pickled_results(files)

    _assert_properties_as_expected()
    assert glob.glob(os.path.join(tangos_results_dir, "*.pickle")) == []


def test_pickled_results_preserve_original_creator(fresh_database, tangos_results_dir, monkeypatch):
    monkeypatch.setattr("sys.argv", ["tangos", "write", "dummy_property", "--pickle-results"])
    run_writer_with_args("dummy_property", "--pickle-results")
    files = sorted(glob.glob(os.path.join(tangos_results_dir, "*.pickle")))

    monkeypatch.setattr("sys.argv", ["tangos", "write-pickled-results", "tangos_results/*.pickle"])
    _commit_pickled_results(files)

    prop = db.get_halo("dummy_sim_1/step.1/1").get_objects("dummy_property")[0]
    assert "write" in prop.creator.command_line
    assert "write-pickled-results" not in prop.creator.command_line


def test_pickle_results_respects_custom_path(fresh_database, tmp_path):
    custom_dir = str(tmp_path / "custom_pickle_dir")

    run_writer_with_args("dummy_property", "--pickle-results", custom_dir)

    files = sorted(glob.glob(os.path.join(custom_dir, "*.pickle")))
    assert len(files) > 0
    assert glob.glob("tangos_results/*.pickle") == []

    _commit_pickled_results(files)

    _assert_properties_as_expected()
    assert glob.glob(os.path.join(custom_dir, "*.pickle")) == []


def test_no_files_deleted_if_a_commit_fails(fresh_database, tangos_results_dir):
    run_writer_with_args("dummy_property", "--pickle-results")
    files = sorted(glob.glob(os.path.join(tangos_results_dir, "*.pickle")))
    assert len(files) > 0

    with open(os.path.join(tangos_results_dir, "corrupt.pickle"), "wb") as f:
        f.write(b"not a valid pickle")
    files.append(os.path.join(tangos_results_dir, "corrupt.pickle"))

    with pytest.raises(Exception):
        _commit_pickled_results(files)

    # since the batch of commits didn't all succeed, nothing should have been deleted
    assert sorted(glob.glob(os.path.join(tangos_results_dir, "*.pickle"))) == sorted(files)
