#!/usr/bin/env python2.7
import halo_db
import halo_db.core.simulation
import halo_db.core.timestep
from halo_db import parallel_tasks
from halo_db.crosslink import need_crosslink_ts, crosslink_ts
from halo_db.tools import terminalcontroller


def crosslink_sim(sim1, sim2, force=False):


    assert sim1 != sim2, "Can't link simulation to itself"
    terminalcontroller.heading("ALL: %s -> %s" % (sim1, sim2))
    ts1s = sim1.timesteps
    ts2s = sim2.timesteps
    tasks = parallel_tasks.distributed(ts1s)
    

    for ts1 in tasks:
        ts2 = min(ts2s, key=lambda ts2: abs(ts2.time_gyr - ts1.time_gyr))
        terminalcontroller.heading(
            "%.2e Gyr -> %.2e Gyr" % (ts1.time_gyr, ts2.time_gyr))
        if need_crosslink_ts(ts1,ts2) or force:
            crosslink_ts(ts1, ts2)

def run():
    import sys
    import halo_db as db

    session = db.core.get_default_session()
    parallel_tasks.mpi_sync_db(session)

    ts1 = halo_db.get_item(sys.argv[1])
    ts2 = halo_db.get_item(sys.argv[2])

    if isinstance(ts1, halo_db.core.simulation.Simulation) and isinstance(ts2, halo_db.core.simulation.Simulation):
        crosslink_sim(ts1, ts2)
    elif isinstance(ts1, halo_db.core.timestep.TimeStep) and isinstance(ts2, halo_db.core.timestep.TimeStep):
        crosslink_ts(ts1, ts2)
    else:
        print "Sorry, couldn't work out what to do with your arguments"


if __name__ == "__main__":
    parallel_tasks.launch(run)
