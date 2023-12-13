import pytest

import tangos
import tangos.testing as testing
import tangos.testing.db_diff as diff
import tangos.testing.simulation_generator
import tangos.tools.db_importer


@pytest.fixture
def source_engine_and_session():
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
    engine, session = tangos.core.get_default_engine(), tangos.get_default_session()
    yield engine, session
    session.close()
    engine.dispose()

@pytest.fixture()
def destination_engine_and_session():
    testing.init_blank_db_for_testing(testing_db_name="imported_db_test")
    _populate_existing_simulation()
    engine, session = tangos.core.get_default_engine(), tangos.get_default_session()
    yield engine, session
    session.close()
    engine.dispose()


def _populate_existing_simulation():
    creator = tangos.testing.simulation_generator.SimulationGeneratorForTests("sim_existing")
    creator.add_timestep()
    creator.add_objects_to_timestep(3)
    creator.add_properties_to_halos(prop=lambda i: i)
    creator.add_properties_to_halos(Mvir=lambda i: i * 100.0)
    creator.add_timestep()
    creator.add_objects_to_timestep(3)
    creator.add_properties_to_halos(prop=lambda i: i * 2)
    creator.link_last_halos()


def test_import(source_engine_and_session, destination_engine_and_session):
    source_engine, source_session = source_engine_and_session
    destination_engine, destination_session = destination_engine_and_session

    assert tangos.get_default_session() is destination_session

    importer = _get_importer_instance(source_engine)

    importer.run_calculation_loop()

    differ = diff.TangosDbDiff(source_session, destination_session)
    differ.compare_simulation("sim")

    assert not differ.failed, "Copied database differs; see log for details"

    # now check that the import process didn't corrupt the existing database
    testing.init_blank_db_for_testing(testing_db_name='existing_db_comparision')
    try:
        _populate_existing_simulation()
        reference_session = tangos.get_default_session()
        differ = diff.TangosDbDiff(reference_session, destination_session)
        differ.compare_simulation("sim_existing")
    finally:
        tangos.core.close_db()

    assert not differ.failed, "Import process has corrupted existing simulation in database; see log for details"


def _get_importer_instance(source_engine, *args):
    importer = tangos.tools.db_importer.DBImporter()
    importer.parse_command_line((source_engine.url,) + args)
    importer.options.files = [source_engine]
    # this looks weird, but it's just making the importer use the engine that we already made
    # if we pass only the URL, it gets cast to a string which makes any passwords into
    # strings of asterisks, which then get passed to the engine and cause it to fail
    return importer


def test_import_filtered(source_engine_and_session, destination_engine_and_session):
    existing_engine, existing_session = source_engine_and_session
    _, destination_session = destination_engine_and_session

    assert tangos.get_default_session() is destination_session




    importer = _get_importer_instance(existing_engine, "--exclude-properties", "Mvir")
    importer.run_calculation_loop()

    assert "Mvir" not in tangos.get_halo("sim/ts1/halo_1").keys()
    assert "Mvir" in tangos.get_halo("sim_existing/ts1/halo_1").keys()
