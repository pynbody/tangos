import os
import pynbody
import numpy as np
import tangos
import tangos.input_handlers.pynbody
from tangos import testing, input_handlers, tools, log, parallel_tasks
import numpy.testing as npt

def setup():

    testing.init_blank_db_for_testing()
    tangos.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")

    manager = tools.add_simulation.SimulationAdderUpdater(input_handlers.pynbody.ChangaInputHandler("test_ahf_merger_tree"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()

def test_property_import():
    importer = tools.property_importer.PropertyImporter()
    importer.parse_command_line("Xc Yc Zc Mvir --for test_ahf_merger_tree".split())
    with log.LogCapturer():
        parallel_tasks.use('multiprocessing')
        parallel_tasks.launch(importer.run_calculation_loop,2)

    Mvir_test, = tangos.get_timestep("test_ahf_merger_tree/tiny_000832").calculate_all("Mvir")
    npt.assert_allclose(Mvir_test, [2.34068e+11,   4.94677e+10,   4.58779e+10,   4.24798e+10,   2.31297e+10,   2.1545e+10,   1.85704e+10,   1.62538e+10,   1.28763e+10])

def test_ahf_merger_tree_import():
    importer = tools.ahf_merger_tree_importer.AHFTreeImporter()
    importer.parse_command_line("--for test_ahf_merger_tree".split())
    with log.LogCapturer():
        importer.run_calculation_loop()

    assert (np.array([x.finder_id for x in tangos.get_timestep("test_ahf_merger_tree/tiny_000832").halos.all()])==[1, 2, 3, 4, 5, 6, 7, 8, 9]).all()
    testing.assert_halolists_equal(tangos.get_timestep("test_ahf_merger_tree/tiny.000832").calculate_all("earlier(1)", object_typetag='halo')[0],
                                   ["test_ahf_merger_tree/tiny.000640/halo_1", "test_ahf_merger_tree/tiny.000640/halo_2",
                                    "test_ahf_merger_tree/tiny.000640/halo_3", "test_ahf_merger_tree/tiny.000640/halo_4",
                                    "test_ahf_merger_tree/tiny.000640/halo_5", "test_ahf_merger_tree/tiny.000640/halo_6",
                                    "test_ahf_merger_tree/tiny.000640/halo_1", "test_ahf_merger_tree/tiny.000640/halo_2"])

    assert tangos.get_halo("%/%640/halo_7").next == tangos.get_halo("%/%832/halo_1")

    assert tangos.get_halo("%/%832/halo_1").previous == tangos.get_halo("%/%640/halo_1")
