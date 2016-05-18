import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db
import halo_db.relation_finding_strategies as relation_finding_strategies

def setup():
    db.init_db("sqlite://")

    session = db.core.get_default_session()

    sim = halo_db.core.simulation.Simulation("sim")
    session.add(sim)

    ts1 = halo_db.core.timestep.TimeStep(sim, "ts1", False)
    session.add(ts1)

    halo_1 = halo_db.core.halo.Halo(ts1, 1, 0, 0, 0, 0)
    bh_1 = halo_db.core.halo.BH(ts1, 2)
    bh_2 = halo_db.core.halo.Halo(ts1, 3, 0, 0, 0, 1)
    session.add_all([halo_1, bh_1, bh_2])

    halo_db.get_halo(1)['BH'] = halo_db.get_halo(2), halo_db.get_halo(3)

    session.commit()

def test_bh_identity():
    assert isinstance(halo_db.get_halo(1), halo_db.core.halo.Halo)
    assert not isinstance(halo_db.get_halo(1), halo_db.core.halo.BH)
    assert isinstance(halo_db.get_halo(2), halo_db.core.halo.BH)
    assert isinstance(halo_db.get_halo(3), halo_db.core.halo.BH)

def test_bh_mapping():
    assert halo_db.get_halo(2) in halo_db.get_halo(1)['BH']
    assert halo_db.get_halo(3) in halo_db.get_halo(1)['BH']
