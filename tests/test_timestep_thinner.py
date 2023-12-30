from pytest import fixture

import tangos
from tangos import testing
from tangos.testing import simulation_generator
from tangos.tools import timestep_thinner


@fixture
def fresh_database():
    testing.init_blank_db_for_testing()
    generator = simulation_generator.SimulationGeneratorForTests()


    times = [0.1, 0.2, 0.3, 0.31, 0.4, 0.5]

    for t in times:
        generator.add_timestep(time = t)
        generator.add_objects_to_timestep(3)
        generator.add_properties_to_halos(test_property = lambda i: i*t)
        if t>0.11:
            generator.link_last_halos()

    _assert_everything_present()

    yield

    tangos.core.close_db()


def _assert_everything_present():
    session = tangos.core.get_default_session()
    assert len(tangos.get_simulation("sim").timesteps) == 6
    assert tangos.get_timestep("sim/ts4").time_gyr == 0.31
    assert session.query(tangos.core.SimulationObjectBase).count() == 18
    assert session.query(tangos.core.HaloProperty).count() == 18
    assert session.query(tangos.core.HaloLink).count() == 30


def _assert_timestep_removed(target_timestep_id):
    session = tangos.core.get_default_session()
    expected_times = [0.1, 0.2, 0.3, 0.4, 0.5]
    for i, t in enumerate(expected_times):
        ts = tangos.get_simulation("sim").timesteps[i]
        assert ts.time_gyr == t
        assert len(ts.objects.all()) == 3
        assert ts.objects[0]['test_property'] == t

        if i > 0:
            assert (ts[1].previous is None and ts.time_gyr == 0.4) or \
                   (ts[1].previous == tangos.get_simulation("sim").timesteps[i - 1][1])
    # finally check there are no orphan objects
    assert session.query(tangos.core.SimulationObjectBase).count() == 15
    assert (tangos.core.get_default_session().query(tangos.core.SimulationObjectBase)
            .filter_by(timestep_id=target_timestep_id).count() == 0)
    # indirectly check that the haloproperties are also gone, just by counting them:
    assert session.query(tangos.core.HaloProperty).count() == 15
    # and the halo links:
    assert session.query(tangos.core.HaloLink).count() == 18


def test_timestep_thinner_no_thinning(fresh_database):

    tt = timestep_thinner.TimestepThinner()
    tt.parse_command_line(["-r", "0.05", "-f"])
    tt.run_calculation_loop()

    _assert_everything_present()


def test_timestep_thinner_relative(fresh_database):
    _assert_everything_present()

    target_ts_id = tangos.get_timestep("sim/ts4").id

    tt = timestep_thinner.TimestepThinner()
    tt.parse_command_line(["-r", "0.5","-f"])
    tt.run_calculation_loop()

    _assert_timestep_removed(target_ts_id)

def test_timestep_thinner_doesnt_over_thin(fresh_database):
    """Check that when the threshold delta_time is more than all the delta times, we retain
    some timesteps, just not spaced more regularly than delta_time"""
    _assert_everything_present()

    tt = timestep_thinner.TimestepThinner()
    tt.parse_command_line(["0.1999", "-f"])
    tt.run_calculation_loop()

    assert [t.extension for t in tangos.get_simulation("sim").timesteps] == ["ts1", "ts3", "ts6"]

def test_timestep_thinner_absolute(fresh_database):
    _assert_everything_present()

    target_id = tangos.get_timestep("sim/ts4").id
    tt = timestep_thinner.TimestepThinner()
    tt.parse_command_line(["0.05", "-f"])
    tt.run_calculation_loop()

    _assert_timestep_removed(target_id)
