from __future__ import absolute_import
import os

import numpy.testing as npt

import tangos as db
import tangos.core.simulation
import tangos.core.timestep
import tangos.input_handlers.halo_stat_files as stat
from tangos import testing


def setup():
    global ts1, ts2
    testing.init_blank_db_for_testing()
    db.config.base = os.path.dirname(os.path.abspath(__file__))+"/"

    session = db.core.get_default_session()

    sim = tangos.core.simulation.Simulation("test_stat_files")
    session.add(sim)

    ts1 = tangos.core.timestep.TimeStep(sim, "pioneer50h128.1536gst1.bwK1.000832")
    ts1.time_gyr = 6  # unimportant
    ts1.redshift = 2.323  # matters for AHF filename

    ts2 = tangos.core.timestep.TimeStep(sim, "h242.cosmo50PLK.1536g1bwK1C52.004096")
    ts2.time_gyr = 10
    ts2.redshift = 0

    session.add_all([ts1,ts2])


    session.commit()

def test_statfile_identity():
    global ts1,ts2
    assert isinstance(stat.HaloStatFile(ts1), stat.AHFStatFile)
    assert isinstance(stat.HaloStatFile(ts2), stat.AmigaIDLStatFile)

def test_ahf_values():
    h_id, ngas, nstar, ndm, ntot,  rvir = stat.HaloStatFile(ts1).read("n_gas", "n_star", "n_dm", "npart", "Rvir")
    assert all(h_id==[1,2,3,4])
    assert all(ngas==[324272,  47634,  53939,  19920])
    assert all(nstar==[1227695,   55825,   24561,    7531])
    assert all(ntot==[5900575,  506026,  498433,  226976])
    assert all(ndm==[4348608, 402567, 419933,  199525])
    npt.assert_allclose(rvir, [195.87, 88.75, 90.01, 69.41])

def test_idl_values():
    h_id, ntot, mvir = stat.HaloStatFile(ts2).read("npart","Mvir")
    assert all(h_id==[1,49,52,58,94,121,127,148,163])
    assert all(ntot==[3273314, 27631, 24654, 22366, 12915, 9831, 9498, 8200, 7256])
    npt.assert_almost_equal(mvir,   [  1.12282280e+12 ,  1.19939950e+10 ,  1.19538740e+10  , 3.07825010e+10,
                                       1.76325820e+10 ,  1.33353700e+10  , 1.28836660e+10 ,  1.11229900e+10,
                                       9.84248220e+09], decimal=5)


def test_insert_halos():
    stat.HaloStatFile(ts1).add_halos(min_NDM=200000)
    db.core.get_default_session().commit()
    assert ts1.halos.count()==3
    assert ts1.halos[0].NDM==4348608
    assert ts1.halos[1].NDM==402567

def test_insert_properties():
    stat.HaloStatFile(ts1).add_halo_properties("Mvir","Rvir")
    npt.assert_almost_equal(ts1.halos[0]["Rvir"], 195.87)
    npt.assert_almost_equal(ts1.halos[0]["Mvir"], 5.02432e+11)