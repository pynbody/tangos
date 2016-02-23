import halo_db as db

def setup():
    db.init_db("sqlite://")

    session = db.core.internal_session

    sim = db.Simulation("sim")

    session.add(sim)

    ts1 = db.TimeStep(sim,"ts1",False)
    ts2 = db.TimeStep(sim,"ts2",False)
    ts3 = db.TimeStep(sim,"ts3",False)

    session.add_all([ts1,ts2,ts3])

    ts1.time_gyr = 1
    ts2.time_gyr = 2
    ts3.time_gyr = 3

    ts1.redshift = 10
    ts2.redshift = 5
    ts3.redshift = 0

    ts1_h1 = db.Halo(ts1,1,1000,0,0,0)
    ts1_h2 = db.Halo(ts1,2,900,0,0,0)
    ts1_h3 = db.Halo(ts1,3,800,0,0,0)
    ts1_h4 = db.Halo(ts1,4,300,0,0,0)

    session.add_all([ts1_h1,ts1_h2,ts1_h3,ts1_h4])

    ts2_h1 = db.Halo(ts2,1,1000,0,0,0)
    ts2_h2 = db.Halo(ts2,2,900,0,0,0)
    ts2_h3 = db.Halo(ts2,3,800,0,0,0)
    ts2_h4 = db.Halo(ts2,4,300,0,0,0)

    session.add_all([ts2_h1,ts2_h2,ts2_h3,ts2_h4])

    ts3_h1 = db.Halo(ts3,1,2000,0,0,0)
    ts3_h2 = db.Halo(ts3,2,800,0,0,0)
    ts3_h3 = db.Halo(ts3,3,300,0,0,0)

    session.add_all([ts3_h1,ts3_h2,ts3_h3])

    rel = db.get_or_create_dictionary_item(session, "ptcls_in_common")

    session.add_all([db.HaloLink(ts1_h1,ts2_h1,rel,1.0)])
    session.add_all([db.HaloLink(ts1_h2,ts2_h2,rel,1.0)])
    session.add_all([db.HaloLink(ts1_h3,ts2_h3,rel,1.0)])
    session.add_all([db.HaloLink(ts1_h4,ts2_h4,rel,1.0)])

    session.add_all([db.HaloLink(ts2_h1,ts3_h1,rel,1.0)])
    session.add_all([db.HaloLink(ts2_h2,ts3_h2,rel,1.0)])
    session.add_all([db.HaloLink(ts2_h3,ts3_h3,rel,1.0)])

    for i,h in enumerate([ts1_h1,ts1_h2,ts1_h3,ts1_h4,ts2_h1,ts2_h2,ts2_h3,ts2_h4,ts3_h1,ts3_h2,ts3_h3]):
        h['Mvir'] = float(i)


def test_gather_property():
    Mv = db.get_timestep("sim/ts1").gather_property("Mvir")
    print Mv
    assert False