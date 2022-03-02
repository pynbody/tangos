import tangos.testing.simulation_generator
from tangos import parallel_tasks as pt
from tangos import testing
import tangos
from sqlalchemy import inspect

def setup():
    pt.use("multiprocessing")
    testing.init_blank_db_for_testing(timeout=5.0, verbose=False)

    generator = tangos.testing.simulation_generator.TestSimulationGenerator()
    generator.add_timestep()
    generator.add_objects_to_timestep(9)

    tangos.core.get_default_session().commit()

def teardown():
    tangos.core.close_db()
    pt.launch(tangos.core.close_db, 6)

def test_database_update():
    tangos.core._check_and_upgrade_database(tangos.core.get_default_engine(), 'test_add_column')
    inspector = inspect(tangos.core.get_default_engine())
    if 'halos' in inspector.get_table_names():
        cols = inspector.get_columns('halos')
        assert 'test_add_column' in [c['name'] for c in cols]

