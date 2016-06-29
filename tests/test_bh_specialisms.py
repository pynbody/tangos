import tangos as db
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos
import tangos.testing
import tangos.relation_finding as relation_finding_strategies

def setup():
    tangos.testing.init_blank_db_for_testing()

    generator = tangos.testing.TestSimulationGenerator()
    generator.add_timestep()
    halo_1, = generator.add_halos_to_timestep(1)
    bh_1, bh_2 = generator.add_bhs_to_timestep(2)

    halo_1['BH'] = bh_2, bh_1

    db.core.get_default_session().commit()

def test_bh_identity():
    assert isinstance(tangos.get_halo(1), tangos.core.halo.Halo)
    assert not isinstance(tangos.get_halo(1), tangos.core.halo.BH)
    assert isinstance(tangos.get_halo(2), tangos.core.halo.BH)
    assert isinstance(tangos.get_halo(3), tangos.core.halo.BH)

def test_bh_mapping():
    assert tangos.get_halo(2) in tangos.get_halo(1)['BH']
    assert tangos.get_halo(3) in tangos.get_halo(1)['BH']
