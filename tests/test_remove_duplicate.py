import tangos
from tangos import core, testing
from tangos.cached_writer import create_property
from tangos.scripts.manager import remove_duplicates
from tangos.testing.simulation_generator import SimulationGeneratorForTests
from tangos.core.halo_data import link

def setup_module():
    testing.init_blank_db_for_testing()
    generator = SimulationGeneratorForTests()

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

    # Also create links between halos, including duplicates
    halo2 = tangos.get_halo(2)
    halo3 = tangos.get_halo(3)
    halo9 = tangos.get_halo(9)

    # twice halo 1 to halo 2
    d_test = tangos.core.get_or_create_dictionary_item(session, "test")
    l_obj = link.HaloLink(halo, halo2, d_test, 1.0)
    session.add(l_obj)
    d_test = tangos.core.get_or_create_dictionary_item(session, "test")
    l_obj = link.HaloLink(halo, halo2, d_test, 1.0)
    session.add(l_obj)

    # once halo 1 to halo 3
    d_test = tangos.core.get_or_create_dictionary_item(session, "test")
    l_obj = link.HaloLink(halo, halo3, d_test, 1.0)
    session.add(l_obj)

    # once halo 2 to halo 9
    d_test = tangos.core.get_or_create_dictionary_item(session, "test")
    l_obj = link.HaloLink(halo2, halo9, d_test, 1.0)
    session.add(l_obj)


def teardown_module():
    core.close_db()


def test():
    # Before cleaning: we have two properties for halo 1
    # and one property for halo 2 and others
    halo = tangos.get_halo(1)
    assert halo["Mvir"] == [-2., -1., 1.]
    for ihalo in range(2, 10):
        halo = tangos.get_halo(ihalo)
        assert halo["Mvir"] == ihalo

    # And three links for halo 1 and one for halo 2
    assert tangos.get_halo(1).links.count() == 3
    assert tangos.get_halo(2).links.count() == 1

    # Let's cleanup
    remove_duplicates(None)

    # After cleaning: we have one property for all halos
    halo = tangos.get_halo(1)
    assert halo["Mvir"] == -2.
    for ihalo in range(2, 10):
        halo = tangos.get_halo(ihalo)
        assert halo["Mvir"] == ihalo

    # And two links for halo 1, pointing to different halos, still one for halo 2
    assert tangos.get_halo(1).links.count() == 2
    assert tangos.get_halo(2).links.count() == 1