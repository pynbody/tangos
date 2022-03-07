from tangos import testing
from tangos.testing.simulation_generator import TestSimulationGenerator
from tangos import core
from tangos.cached_writer import create_property
import tangos

from tangos.scripts.manager import remove_duplicates


def setup():
    testing.init_blank_db_for_testing()
    generator = TestSimulationGenerator()

    generator.add_timestep()
    generator.add_objects_to_timestep(10)

    generator.add_properties_to_halos(Mvir=lambda i: 1. * i)

    halo = tangos.get_halo(1)

    session = core.get_default_session()
    px = create_property(halo, "Mvir", -1., session)
    session.add(px, session)
    px = create_property(halo, "Mvir", -2., session)
    session.add(px, session)
    session.commit()


def teardown():
    core.close_db()


def test():
    # Before cleaning: we have two properties for halo 1
    # and one property for halo 2 and others
    halo = tangos.get_halo(1)
    assert halo["Mvir"] == [-2., -1., 1.]
    for ihalo in range(2, 10):
        halo = tangos.get_halo(ihalo)
        assert halo["Mvir"] == ihalo

    # Let's cleanup
    remove_duplicates(None)

    # After cleaning: we have one property for all halos
    halo = tangos.get_halo(1)
    assert halo["Mvir"] == -2.
    for ihalo in range(2, 10):
        halo = tangos.get_halo(ihalo)
        assert halo["Mvir"] == ihalo
