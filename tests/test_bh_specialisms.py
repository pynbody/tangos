import halo_db as db
import halo_db.halo_finder as halo_finder

def setup():
    db.init_db("sqlite://")

    session = db.core.internal_session

    sim = db.Simulation("sim")
    session.add(sim)

    ts1 = db.TimeStep(sim,"ts1",False)
    session.add(ts1)

    halo_1 = db.core.Halo(ts1,1,0,0,0,0)
    bh_1 = db.core.BH(ts1,2)
    bh_2 = db.core.Halo(ts1,3,0,0,0,1)
    session.add_all([halo_1, bh_1, bh_2])

    db.get_halo(1)['BH'] = db.get_halo(2), db.get_halo(3)

    session.commit()

def test_bh_identity():
    assert isinstance(db.get_halo(1), db.core.Halo)
    assert not isinstance(db.get_halo(1), db.core.BH)
    assert isinstance(db.get_halo(2), db.core.BH)
    assert isinstance(db.get_halo(3), db.core.BH)

def test_bh_mapping():
    assert db.get_halo(2) in db.get_halo(1)['BH']
    assert db.get_halo(3) in db.get_halo(1)['BH']
