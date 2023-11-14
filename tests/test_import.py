import tangos
import tangos.tools.db_importer
import tangos.testing as testing
import tangos.testing.db_diff as diff
import tangos.testing.simulation_generator


def setup_module():
    testing.init_blank_db_for_testing()

    creator = tangos.testing.simulation_generator.SimulationGeneratorForTests()

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

def teardown_module():
    tangos.core.close_db()


def test_import():
    source_engine = tangos.core.get_default_engine()
    source_session = tangos.get_default_session()
    testing.init_blank_db_for_testing(testing_db_name='imported_db_test')
    _populate_existing_simulation()

    importer = tangos.tools.db_importer.DBImporter()
    importer.parse_command_line([source_engine.url])
    importer.run_calculation_loop()

    new_session = tangos.get_default_session()
    differ = diff.TangosDbDiff(source_session, new_session)
    differ.compare_simulation("sim")

    assert not differ.failed, "Copied database differs; see log for details"

    # now check that the import process didn't corrupt the existing database
    testing.init_blank_db_for_testing(testing_db_name='existing_db_comparision')
    _populate_existing_simulation()
    reference_session = tangos.get_default_session()
    differ = diff.TangosDbDiff(new_session, reference_session)
    differ.compare_simulation("sim_existing")

    assert not differ.failed, "Import process has corrupted existing simulation in database; see log for details"


def _populate_existing_simulation():
    creator = tangos.testing.simulation_generator.SimulationGeneratorForTests("sim_existing")
    creator.add_timestep()
    creator.add_objects_to_timestep(3)
    creator.add_properties_to_halos(prop=lambda i: i)
    creator.add_properties_to_halos(Mvir=lambda i: i*100.0)
    creator.add_timestep()
    creator.add_objects_to_timestep(3)
    creator.add_properties_to_halos(prop=lambda i: i*2)
    creator.link_last_halos()


def test_import_filtered():
    existing_engine = tangos.core.get_default_engine()
    testing.init_blank_db_for_testing(testing_db_name='imported_db_test')
    _populate_existing_simulation()

    importer = tangos.tools.db_importer.DBImporter()
    importer.parse_command_line([existing_engine.url, "--exclude-properties", "Mvir"])
    importer.run_calculation_loop()

    assert "Mvir" not in tangos.get_halo("sim/ts1/halo_1").keys()
    assert "Mvir" in tangos.get_halo("sim_existing/ts1/halo_1").keys()