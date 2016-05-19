import halo_db as db
import halo_db.core.halo
import halo_db.core.simulation
import halo_db.core.timestep
import halo_db
import halo_db.testing
import halo_db.relation_finding_strategies as relation_finding_strategies

def setup():
    db.init_db("sqlite://")

    generator = halo_db.testing.TestDatabaseGenerator()
    generator.add_timestep()
    halo_1, = generator.add_halos_to_timestep(1)
    bh_1, bh_2 = generator.add_bhs_to_timestep(2)

    halo_1['BH'] = bh_2, bh_1

    db.core.get_default_session().commit()

def test_bh_identity():
    assert isinstance(halo_db.get_halo(1), halo_db.core.halo.Halo)
    assert not isinstance(halo_db.get_halo(1), halo_db.core.halo.BH)
    assert isinstance(halo_db.get_halo(2), halo_db.core.halo.BH)
    assert isinstance(halo_db.get_halo(3), halo_db.core.halo.BH)

def test_bh_mapping():
    assert halo_db.get_halo(2) in halo_db.get_halo(1)['BH']
    assert halo_db.get_halo(3) in halo_db.get_halo(1)['BH']
