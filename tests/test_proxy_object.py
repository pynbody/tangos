from pytest import raises as assert_raises

import tangos
import tangos as db
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos.testing
import tangos.testing.simulation_generator
import tangos.util.proxy_object as po
import tangos.util.timestep_object_cache as toc


def setup_module():
    tangos.testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    generator.add_timestep()
    generator.add_objects_to_timestep(1)
    generator.add_bhs_to_timestep(2)

    generator.add_timestep()
    generator.add_objects_to_timestep(1)

    db.core.get_default_session().commit()

def teardown_module():
    tangos.core.close_db()

def test_proxy_object_by_id():
    assert po.ProxyObjectFromDatabaseId(1).resolve(tangos.get_default_session())==tangos.get_object(1)

def test_incomplete_proxy_object():
    with assert_raises(po.ProxyResolutionException):
        po.IncompleteProxyObjectFromFinderId(1,'halo').resolve(tangos.get_default_session())

def test_proxy_object_from_id_and_timestep():
    incomplete = po.IncompleteProxyObjectFromFinderId(1, 'halo')
    assert incomplete.relative_to_timestep_id(1).resolve(tangos.get_default_session())==tangos.get_object('sim/ts1/halo_1')
    assert incomplete.relative_to_timestep_id(2).resolve(tangos.get_default_session()) == tangos.get_object(
        'sim/ts2/halo_1')

    incomplete = po.IncompleteProxyObjectFromFinderId(1, 'BH')
    assert incomplete.relative_to_timestep_id(1).resolve(tangos.get_default_session()) == tangos.get_object(
        'sim/ts1/BH_1')

def test_proxy_object_from_id_and_timestep_cache():
    ts1_halo1 = tangos.get_object('sim/ts1/halo_1')
    ts1_bh2 = tangos.get_object('sim/ts1/BH_2')
    with tangos.testing.SqlExecutionTracker() as ctr:
        incomplete = po.IncompleteProxyObjectFromFinderId(1, 'halo')
        cache = toc.TimestepObjectCache(tangos.get_timestep('sim/ts1'))
        assert incomplete.relative_to_timestep_cache(cache).resolve(tangos.get_default_session()) == ts1_halo1

        incomplete = po.IncompleteProxyObjectFromFinderId(2, 'BH')
        assert incomplete.relative_to_timestep_cache(cache).resolve(tangos.get_default_session()) == ts1_bh2

    assert ctr.count_statements_containing("SELECT halos")==1


def test_unresolvable_proxy_object():
    incomplete = po.IncompleteProxyObjectFromFinderId(10, 'halo')
    assert incomplete.relative_to_timestep_id(1).resolve(tangos.get_default_session()) is None

    cache = toc.TimestepObjectCache(tangos.get_timestep('sim/ts1'))
    assert incomplete.relative_to_timestep_cache(cache).resolve(tangos.get_default_session()) is None
