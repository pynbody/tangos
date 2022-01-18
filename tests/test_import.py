import tangos, tangos.testing as testing, tangos.scripts.manager as manager
import tangos.testing.simulation_generator
import tangos.testing.db_diff as diff

from nose.plugins.skip import SkipTest
import os

def setup():
    testing.init_blank_db_for_testing()

    creator = tangos.testing.simulation_generator.TestSimulationGenerator()

    halo_offset = 0
    for ts in range(1,4):
        num_halos = 4 if ts<3 else 3
        creator.add_timestep()
        creator.add_objects_to_timestep(num_halos)
        creator.add_properties_to_halos(Mvir=lambda i: i+halo_offset)
        creator.add_properties_to_halos(Rvir=lambda i: (i+halo_offset)*0.1)
        halo_offset+=num_halos

        creator.add_bhs_to_timestep(4)
        creator.add_properties_to_bhs(hole_mass = lambda i: float(i*100))
        creator.add_properties_to_bhs(hole_spin = lambda i: 1000-float(i)*100)

        creator.assign_bhs_to_halos({1:1, 2:2, 3:3, 4:3})


        if ts>1:
            creator.link_last_halos()
            creator.link_last_bhs_using_mapping({1:1})

def test_import():
    if os.environ.get("DB_BACKEND", "sqlite") != "sqlite":
        raise SkipTest("Skipping for MySQL databases")
    existing_session = tangos.get_default_session()
    testing.init_blank_db_for_testing(testing_db_name='imported_db_test')
    new_session = tangos.get_default_session()
    manager._db_import_export(new_session, existing_session)
    differ = diff.TangosDbDiff(existing_session, new_session)
    differ.compare()
    assert not differ.failed, "Copied database differs; see log for details"