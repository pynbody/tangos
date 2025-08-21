import gc
import os

import numpy as np
import numpy.testing as npt
import pynbody

import tangos
import tangos as db
import tangos.input_handlers.pynbody as pynbody_outputs
import tangos.tools.add_simulation as add
from tangos import config, log, testing


def setup_module():
    global output_manager
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    output_manager = pynbody_outputs.ChangaInputHandler("test_tipsy")

def teardown_module():
    tangos.core.close_db()

def test_get_handler():
    assert db.input_handlers.get_named_handler_class('pynbody.ChangaInputHandler') == pynbody_outputs.ChangaInputHandler

def test_get_deprecated_handler():
    assert db.input_handlers.get_named_handler_class('pynbody.ChangaOutputSetHandler') == pynbody_outputs.ChangaInputHandler
    
def test_get_multi_level_handler():
    assert db.input_handlers.get_named_handler_class('tangos.input_handlers.pynbody.ChangaInputHandler') == pynbody_outputs.ChangaInputHandler
    assert db.input_handlers.get_named_handler_class('tangos.input_handlers.pynbody.ChangaOutputSetHandler') == pynbody_outputs.ChangaInputHandler

def test_handler_name():
    assert pynbody_outputs.ChangaInputHandler.handler_class_name()=="pynbody.ChangaInputHandler"

def test_handler_properties():
    prop = output_manager.get_properties()
    assert len(prop) == 15
    assert 'approx_resolution_kpc' in prop
    assert 'approx_resolution_Msol' in prop
    npt.assert_allclose(prop['approx_resolution_kpc'], 0.3499348849)
    npt.assert_allclose(prop['approx_resolution_Msol'], 144411.17640)

def test_handler_properties_quicker_flag():
    output_manager.quicker = True
    prop = output_manager.get_properties()
    npt.assert_allclose(prop['approx_resolution_kpc'], 33.590757, rtol=1e-5)
    npt.assert_allclose(prop['approx_resolution_Msol'], 2.412033e+10, rtol=1e-4)

def test_enumerate():
    assert set(output_manager.enumerate_timestep_extensions())=={"tiny.000640","tiny.000832"}

def test_timestep_properties():
    props = output_manager.get_timestep_properties("tiny.000640")
    npt.assert_allclose(props['time_gyr'],2.173236752357068)
    npt.assert_allclose(props['redshift'], 2.96382819878)

def test_enumerate_objects():
    halos = list(output_manager.enumerate_objects("tiny.000640"))
    assert len(halos)==9
    assert halos[0]==[1, 0, 2041986, 364232, 198355]
    assert halos[1]==[2, 1, 421027, 30282, 57684]

def test_properties():
    props = output_manager.get_properties()
    assert props['dPhysDenMin']==0.2 # from param file
    assert props['macros'].startswith("CHANGESOFT COOLING_COSMO") # from log file
    assert "dBHSinkAlpha" not in props # in the param file but not in the list of parameters we want to expose

def test_enumerate_objects_using_statfile():
    halos = list(output_manager.enumerate_objects("tiny.000640"))
    assert halos[0]==[1,0, 2041986,364232, 198355]
    assert len(halos)==9

def test_enumerate_objects_using_pynbody():
    config.min_halo_particles = 400
    halos = list(output_manager.enumerate_objects("tiny.000832", min_halo_particles=200))
    # NB one of these halos is actually halo -1 which is the particles not in any halo
    # but right now pynbody is returning that as a 'halo' when using grp arrays, so... we accept that as
    # the 'correct' behaviour since it's a pynbody-dependent thing

    npt.assert_equal(halos[1], [0, 0, 477,80, 48])
    assert len(halos)==2

def test_load_timestep():
    add_test_simulation_to_db()
    pynbody_f = db.get_timestep("test_tipsy/tiny.000640").load()
    assert isinstance(pynbody_f, pynbody.snapshot.SimSnap)
    assert pynbody_f.filename.endswith("tiny.000640")

def test_load_halo():
    add_test_simulation_to_db()
    pynbody_h = db.get_halo("test_tipsy/tiny.000640/1").load()
    assert isinstance(pynbody_h, pynbody.snapshot.IndexedSubSnap)
    assert len(pynbody_h)==200
    assert_is_subview_of_full_file(pynbody_h)

def test_partial_load_halo():
    add_test_simulation_to_db()
    pynbody_h = db.get_halo("test_tipsy/tiny.000640/1").load(mode='partial')
    assert len(pynbody_h) == 200
    assert pynbody_h.ancestor is pynbody_h

def test_load_tracker_halo():
    add_test_simulation_to_db()
    pynbody_h = db.get_halo("test_tipsy/tiny.000640/tracker_1").load()
    assert len(pynbody_h)==4

    # test that we have a subview of the whole file
    assert_is_subview_of_full_file(pynbody_h)

def test_partial_load_tracker_halo():
    add_test_simulation_to_db()
    pynbody_h = db.get_halo("test_tipsy/tiny.000640/tracker_1").load(mode='partial')
    assert len(pynbody_h)==4
    assert pynbody_h.ancestor is pynbody_h

def test_load_persistence():
    add_test_simulation_to_db()
    f = db.get_timestep("test_tipsy/tiny.000640").load()
    f2 = db.get_timestep("test_tipsy/tiny.000640").load()
    h = db.get_halo("test_tipsy/tiny.000640/1").load()
    h_tracker = db.get_halo("test_tipsy/tiny.000640/tracker_1").load()
    assert id(f)==id(f2)
    assert id(h.ancestor)==id(f)
    assert id(h_tracker.ancestor)==id(f)

    old_id = id(f)

    # Enable GC debugging
    gc.set_debug(gc.DEBUG_LEAK | gc.DEBUG_COLLECTABLE | gc.DEBUG_UNCOLLECTABLE)

    del f, f2, h, h_tracker
    gc.collect()

    f3 = db.get_timestep("test_tipsy/tiny.000640").load()
    if id(f3)==old_id:
        print("------ Referrers: ---")
        referrers = gc.get_referrers(f3)
        print("Number of referrers:", len(referrers))
        for i, ref in enumerate(referrers):
            print(f"Referrer {i}: type={type(ref)}, repr={repr(ref)[:200]}")
        print("------ GC objects tracked: ---")
        print("Total objects tracked by GC:", len(gc.get_objects()))
        print("Types of objects tracked (top 10):")
        from collections import Counter
        type_counts = Counter(type(obj) for obj in gc.get_objects())
        for t, c in type_counts.most_common(10):
            print(f"{t}: {c}")
        print("------ End debug info ------")

    gc.set_debug(0)

    assert id(f3)!=old_id

def test_load_tracker_iord_halo():
    add_test_simulation_to_db()
    h_direct = db.get_halo("test_tipsy/tiny.000640/tracker_1").load(mode='partial')
    h_iord = db.get_halo("test_tipsy/tiny.000640/tracker_2").load(mode='partial')
    assert (h_direct['iord']==h_iord['iord']).all()


_added_to_db = False
tracked_particles = [2, 4, 6, 8]
tracked_iord = [20000,40000,60000,80000]

def add_test_simulation_to_db():
    global _added_to_db

    if not _added_to_db:
        with log.LogCapturer():
            add.SimulationAdderUpdater(output_manager).scan_simulation_and_add_all_descendants()
        tx = db.core.tracking.TrackData(db.get_simulation("test_tipsy"))
        tx.particles = tracked_particles
        tx.use_iord = False
        db.core.get_default_session().add(tx)
        tx.create_objects()

        tx = db.core.tracking.TrackData(db.get_simulation("test_tipsy"))
        tx.particles = tracked_iord
        tx.use_iord = True
        db.core.get_default_session().add(tx)
        tx.create_objects()
        _added_to_db=True

def assert_is_subview_of_full_file(pynbody_h):
    assert len(pynbody_h.ancestor) == len(db.get_timestep("test_tipsy/tiny.000640").load())

def test_load_region():
    add_test_simulation_to_db()
    region = db.get_timestep("test_tipsy/tiny.000640").load_region(pynbody.filt.Sphere(2000,[1000,1000,1000]))
    assert (region['iord']==[ 9980437, 10570437, 10630437, 10640437, 10770437, 10890437,
       10900437, 10960437, 11030437, 11090437, 11480437, 11490437,
       11550437, 11740437, 12270437, 12590437, 12600437, 12920437,
       13380437, 13710437]).all()

def test_load_region_uses_cache():
    add_test_simulation_to_db()
    filt1 = pynbody.filt.Sphere(2000,[1000,1000,1000])
    filt2 = pynbody.filt.Sphere(2000,[1001,1000,1000])

    region1a = db.get_timestep("test_tipsy/tiny.000640").load_region(filt1)
    region2 = db.get_timestep("test_tipsy/tiny.000640").load_region(filt2)
    region1b = db.get_timestep("test_tipsy/tiny.000640").load_region(filt1)

    assert id(region1a) == id(region1b)
    assert id(region1a) != id(region2)


class DummyHaloClass(pynbody.halo.number_array.HaloNumberCatalogue):
    def __init__(self, sim):
        sim['grp'] = np.empty(len(sim), dtype=int)
        sim['grp'].fill(-1)
        sim['grp'][:1000] = 0
        sim['grp'][1000:2000] = 1
        super().__init__(sim, 'grp', ignore=-1)

    @classmethod
    def _can_load(cls, sim, arr_name='grp'):
        return True


class DummyPynbodyHandler(pynbody_outputs.ChangaInputHandler):
    pynbody_halo_class_name = "DummyHaloClass"

    def _can_enumerate_objects_from_statfile(self, ts_extension, object_typetag):
        return False # test requires enumerating halos via pynbody, to verify right halo class is used

def test_halo_class_priority():
    with testing.blank_db_for_testing(testing_db_name="test_halo_class_priority", erase_if_exists=True):
        handler = DummyPynbodyHandler("test_tipsy")

        with log.LogCapturer():
            add.SimulationAdderUpdater(handler).scan_simulation_and_add_all_descendants()
        h = db.get_halo("test_tipsy/tiny.000640/1").load()
        assert (h.get_index_list(h.ancestor) == np.arange(1000)).all()
        h = db.get_halo("test_tipsy/tiny.000640/2").load()
        assert (h.get_index_list(h.ancestor) == np.arange(1000, 2000)).all()

def test_input_handler_priority():
    handler = pynbody_outputs.ChangaInputHandler.best_matching_handler("test_tipsy")
    assert handler is DummyPynbodyHandler


    # test that if we specialise further, we get the more specialised handler
    class DummyPynbodyHandler2(DummyPynbodyHandler):
        pass

    handler = pynbody_outputs.ChangaInputHandler.best_matching_handler("test_tipsy")
    assert handler is DummyPynbodyHandler2


    # specialise still further, but disable autoselect so that we don't get this handler returned
    class DummyPynbodyHandler3(DummyPynbodyHandler2):
        enable_autoselect = False
        pass

    handler = pynbody_outputs.ChangaInputHandler.best_matching_handler("test_tipsy")
    assert handler is DummyPynbodyHandler2

    handler = DummyPynbodyHandler2.best_matching_handler("test_tipsy")
    assert handler is DummyPynbodyHandler2

    # if we select handler 3 manually, we should get it
    handler = DummyPynbodyHandler3.best_matching_handler("test_tipsy")
    assert handler is DummyPynbodyHandler3
