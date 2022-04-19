import tangos
import tangos as db
import tangos.core.halo
import tangos.core.simulation
import tangos.core.timestep
import tangos.relation_finding as relation_finding_strategies
import tangos.testing
import tangos.testing.simulation_generator


def setup_module():
    tangos.testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    generator.add_timestep()
    halo_1, = generator.add_objects_to_timestep(1)
    bh_1, bh_2 = generator.add_bhs_to_timestep(2)

    halo_1['BH'] = bh_2, bh_1

    db.core.get_default_session().commit()

def teardown_module():
    tangos.core.close_db()

def test_bh_identity():
    assert isinstance(tangos.get_halo(1), tangos.core.halo.Halo)
    assert not isinstance(tangos.get_halo(1), tangos.core.halo.BH)
    assert isinstance(tangos.get_halo(2), tangos.core.halo.BH)
    assert isinstance(tangos.get_halo(3), tangos.core.halo.BH)

def test_bh_mapping():
    assert tangos.get_halo(2) in tangos.get_halo(1)['BH']
    assert tangos.get_halo(3) in tangos.get_halo(1)['BH']

def test_tags():
    from tangos.core.halo import BH, Group, Halo, SimulationObjectBase, Tracker
    assert SimulationObjectBase.class_from_tag('halo') is Halo
    assert SimulationObjectBase.class_from_tag('BH') is BH
    assert SimulationObjectBase.class_from_tag('group') is Group
    assert SimulationObjectBase.class_from_tag('tracker') is Tracker
    assert SimulationObjectBase.object_typecode_from_tag('halo')==0
    assert SimulationObjectBase.object_typecode_from_tag('BH')==1
    assert SimulationObjectBase.object_typecode_from_tag('group')==2
    assert SimulationObjectBase.object_typecode_from_tag('tracker')==3
    assert SimulationObjectBase.object_typecode_from_tag(1)==1
    assert SimulationObjectBase.object_typetag_from_code(0)=='halo'
    assert SimulationObjectBase.object_typetag_from_code(1)=='BH'
    assert SimulationObjectBase.object_typetag_from_code(2)=='group'
    assert SimulationObjectBase.object_typetag_from_code(3)=='tracker'
