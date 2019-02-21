from __future__ import absolute_import
import tangos as db
import tangos.input_handlers.pynbody as pynbody_outputs
import tangos.tools.add_simulation as add
from tangos import config
from tangos import log, testing
import os
import numpy.testing as npt
import pynbody
import gc

def setup():
    global output_manager
    testing.init_blank_db_for_testing()
    db.config.base = os.path.join(os.path.dirname(__file__), "test_simulations")
    output_manager = pynbody_outputs.ChangaInputHandler("test_tipsy")

def test_get_handler():
    assert db.input_handlers.get_named_handler_class('pynbody.ChangaInputHandler') == pynbody_outputs.ChangaInputHandler

def test_get_deprecated_handler():
    assert db.input_handlers.get_named_handler_class('pynbody.ChangaOutputSetHandler') == pynbody_outputs.ChangaInputHandler

def test_handler_name():
    assert pynbody_outputs.ChangaInputHandler.handler_class_name()=="pynbody.ChangaInputHandler"

def test_handler_properties():
    prop = output_manager.get_properties()
    assert len(prop) == 10
    assert 'approx_resolution_kpc' in prop
    assert 'approx_resolution_Msol' in prop
    npt.assert_allclose(prop['approx_resolution_kpc'], 0.3499348849)
    npt.assert_allclose(prop['approx_resolution_Msol'], 144411.17640)

def test_handler_properties_quicker_flag():
    output_manager.quicker = True
    prop = output_manager.get_properties()
    npt.assert_allclose(prop['approx_resolution_kpc'], 33.4360203909648)
    npt.assert_allclose(prop['approx_resolution_Msol'], 40370039199.44858)

def test_enumerate():
    assert set(output_manager.enumerate_timestep_extensions())==set(["tiny.000640","tiny.000832"])

def test_timestep_properties():
    props = output_manager.get_timestep_properties("tiny.000640")
    npt.assert_allclose(props['time_gyr'],2.17328504831)
    npt.assert_allclose(props['redshift'], 2.96382819878)

def test_enumerate_objects():
    halos = list(output_manager.enumerate_objects("tiny.000640"))
    assert len(halos)==9
    assert halos[0]==[1,2041986, 364232, 198355]
    assert halos[1]==[2, 421027, 30282, 57684]

def test_properties():
    props = output_manager.get_properties()
    assert props['dPhysDenMin']==0.2 # from param file
    assert props['macros'].startswith("CHANGESOFT COOLING_COSMO") # from log file
    assert "dBHSinkAlpha" not in props # in the param file but not in the list of parameters we want to expose

def test_enumerate_objects_using_statfile():
    halos = list(output_manager.enumerate_objects("tiny.000640"))
    assert halos[0]==[1,2041986,364232, 198355]
    assert len(halos)==9

def test_enumerate_objects_using_pynbody():
    config.min_halo_particles = 400
    halos = list(output_manager.enumerate_objects("tiny.000832", min_halo_particles=200))
    npt.assert_equal(halos[0], [1,477,80, 48])
    assert len(halos)==1

def test_load_timestep():
    add_test_simulation_to_db()
    pynbody_f = db.get_timestep("test_tipsy/tiny.000640").load()
    assert isinstance(pynbody_f, pynbody.snapshot.SimSnap)
    assert pynbody_f.filename.endswith("tiny.000640")

def test_load_halo():
    add_test_simulation_to_db()
    pynbody_h = db.get_halo("test_tipsy/tiny.000640/1").load()
    assert isinstance(pynbody_h, pynbody.snapshot.SubSnap)
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
    f = db.get_timestep("test_tipsy/tiny.000640").load()
    f2 = db.get_timestep("test_tipsy/tiny.000640").load()
    h = db.get_halo("test_tipsy/tiny.000640/1").load()
    h_tracker = db.get_halo("test_tipsy/tiny.000640/tracker_1").load()
    assert id(f)==id(f2)
    assert id(h.ancestor)==id(f)
    assert id(h_tracker.ancestor)==id(f)

    old_id = id(f)

    del f, f2, h, h_tracker
    gc.collect()

    f3 = db.get_timestep("test_tipsy/tiny.000640").load()
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
        tx = db.core.get_default_session().merge(tx)
        tx.create_objects()

        tx = db.core.tracking.TrackData(db.get_simulation("test_tipsy"))
        tx.particles = tracked_iord
        tx.use_iord = True
        tx = db.core.get_default_session().merge(tx)
        tx.create_objects()
        _added_to_db=True

def assert_is_subview_of_full_file(pynbody_h):
    assert len(pynbody_h.ancestor) == len(db.get_timestep("test_tipsy/tiny.000640").load())

def test_load_region():
    region = db.get_timestep("test_tipsy/tiny.000640").load_region(pynbody.filt.Sphere(2000,[1000,1000,1000]))
    assert (region['iord']==[ 9980437, 10570437, 10630437, 10640437, 10770437, 10890437,
       10900437, 10960437, 11030437, 11090437, 11480437, 11490437,
       11550437, 11740437, 12270437, 12590437, 12600437, 12920437,
       13380437, 13710437]).all()