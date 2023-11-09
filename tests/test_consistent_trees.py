import os

import numpy as np
import numpy.testing as npt
import pynbody

import tangos
import tangos.input_handlers.pynbody
from tangos import input_handlers, log, parallel_tasks, testing, tools


def _get_gadget_snap_path(snapname):
    return os.path.join(os.path.dirname(__file__),"test_simulations",
                        "test_gadget_rockstar",snapname)

def _create_dummy_simsnap():
    f = pynbody.new(dm=2097152)
    f['iord'] = np.arange(2097152)
    f['pos'] = np.zeros((2097152,3)).view(pynbody.array.SimArray)
    f['pos'].units="Mpc"
    f['vel'] = np.zeros((2097152, 3)).view(pynbody.array.SimArray)
    f['vel'].units = "km s^-1"
    f['mass'] = np.ones(2097152).view(pynbody.array.SimArray)
    f['mass'].units="Msol"
    f.properties['boxsize'] = pynbody.units.Unit("50 Mpc")
    return f


def _ensure_dummy_gadgetsnaps_exist():
    f = None
    if not os.path.exists(_get_gadget_snap_path("snapshot_013")):
        f = _create_dummy_simsnap()
        f.properties['z'] = 2.9322353443127693
        f.write(pynbody.snapshot.gadget.GadgetSnap, _get_gadget_snap_path("snapshot_013"))
    if not os.path.exists(_get_gadget_snap_path("snapshot_014")):
        if not f:
            f = _create_dummy_simsnap()
        f.properties['z'] = 2.2336350508252085
        f.write(pynbody.snapshot.gadget.GadgetSnap, _get_gadget_snap_path("snapshot_014"))


def setup_module():
    _ensure_dummy_gadgetsnaps_exist()

    testing.init_blank_db_for_testing()
    tangos.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")

    manager = tools.add_simulation.SimulationAdderUpdater(input_handlers.pynbody.GadgetRockstarInputHandler("test_gadget_rockstar"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

def teardown_module():
    tangos.core.close_db()


def test_property_import():
    importer = tools.property_importer.PropertyImporter()
    importer.parse_command_line("X Y Z Mvir --for test_gadget_rockstar".split())
    with log.LogCapturer():
        parallel_tasks.use('multiprocessing-2')
        parallel_tasks.launch(importer.run_calculation_loop)

    Mvir_test, = tangos.get_timestep("test_gadget_rockstar/snapshot_013").calculate_all("Mvir")
    npt.assert_allclose(Mvir_test, [1.160400e+13,   8.341900e+12,   5.061400e+12,   5.951900e+12])

def test_consistent_tree_import():
    importer = tools.consistent_trees_importer.ConsistentTreesImporter()
    importer.parse_command_line("--for test_gadget_rockstar --with-ids".split())
    with log.LogCapturer():
        importer.run_calculation_loop()
    assert (tangos.get_timestep("test_gadget_rockstar/snapshot_014").calculate_all("consistent_trees_id", object_typetag='halo')[0]==[17081, 19718, 19129]).all()
    testing.assert_halolists_equal(tangos.get_timestep("test_gadget_rockstar/snapshot_014").calculate_all("earlier(1)", object_typetag='halo')[0],
                                   ["test_gadget_rockstar/snapshot_013/halo_2",
                                    "test_gadget_rockstar/snapshot_013/halo_4"])

    assert tangos.get_halo("%/%13/halo_1").next == tangos.get_halo("%/%14/phantom_1")
