from __future__ import absolute_import
import tangos as db
from tangos.input_handlers import output_testing
from tangos.tools import crosslink, add_simulation
from tangos import log, parallel_tasks, live_calculation, testing
from tangos.core.halo_data import link
from nose.tools import assert_raises
import os, os.path

def setup():
    parallel_tasks.use('null')
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    manager = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_1"))
    manager2 = add_simulation.SimulationAdderUpdater(output_testing.TestInputHandler("dummy_sim_2"))
    with log.LogCapturer():
        manager.scan_simulation_and_add_all_descendants()
        manager2.scan_simulation_and_add_all_descendants()

def test_issue_77():
    # tests that the input handler caching does not deliver the wrong timestep when it has the same name
    ih1 = output_testing.TestInputHandler("dummy_sim_1")
    ih2 = output_testing.TestInputHandler("dummy_sim_2")
    assert(ih1.load_timestep("step.1") is not ih2.load_timestep("step.1"))

def test_timestep_linking():
    tl = crosslink.TimeLinker()
    tl.parse_command_line([])
    with log.LogCapturer():
        tl.run_calculation_loop()
    assert db.get_halo("dummy_sim_1/step.1/1").next==db.get_halo("dummy_sim_1/step.2/1")
    assert db.get_halo("dummy_sim_1/step.2/2").previous == db.get_halo("dummy_sim_1/step.1/2")
    assert db.get_halo("dummy_sim_1/step.1/1").links.count()==2

def test_crosslinking():
    cl = crosslink.CrossLinker()
    cl.parse_command_line(["dummy_sim_2","dummy_sim_1"])

    with log.LogCapturer():
        assert cl.need_crosslink_ts(db.get_timestep("dummy_sim_1/step.1"), db.get_timestep("dummy_sim_2/step.1"))
        cl.run_calculation_loop()
        assert not cl.need_crosslink_ts(db.get_timestep("dummy_sim_1/step.1"), db.get_timestep("dummy_sim_2/step.1"))

    assert db.get_halo('dummy_sim_1/step.1/1').calculate('match("dummy_sim_2").dbid()')==db.get_halo('dummy_sim_2/step.1/1').id
    assert db.get_halo('dummy_sim_2/step.2/3').calculate('match("dummy_sim_1").dbid()') == db.get_halo(
        'dummy_sim_1/step.2/3').id

    with assert_raises(live_calculation.NoResultsError):
        result = db.get_halo('dummy_sim_2/step.3/1').calculate('match("dummy_sim_1").dbid()')


def test_link_repr():
    h1 = db.get_halo('dummy_sim_1/step.1/1')
    h2 = db.get_halo('dummy_sim_1/step.1/2')
    d_test = db.core.get_or_create_dictionary_item(db.get_default_session(), "test")
    l_obj = link.HaloLink(h1, h2, d_test, 1.0)
    assert repr(l_obj)=="<HaloLink test dummy_sim_1/step.1/halo_1 to dummy_sim_1/step.1/halo_2 weight=1.00>"
    l_obj = link.HaloLink(h1, h2, d_test, None)
    assert repr(l_obj) == "<HaloLink test dummy_sim_1/step.1/halo_1 to dummy_sim_1/step.1/halo_2 weight=None>"