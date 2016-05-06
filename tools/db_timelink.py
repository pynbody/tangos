#!/usr/bin/env python2.7


import halo_db as db
import argparse

import halo_db.core.timestep
from halo_db import parallel_tasks, crosslink


def run():
    #db.use_blocking_session()
    session = db.core.get_default_session()



    parser = argparse.ArgumentParser()
    db.supplement_argparser(parser)
    parser.add_argument("--verbose", action="store_true",
                        help="Print extra information")
    parser.add_argument("--force", action="store_true",
                        help="Generate links even if they already exist for those timesteps")
    parser.add_argument("--hmax", action="store",type=int,default=200,
                        help="Specify the maximum number of halos per snapshot")
    parser.add_argument('--sims','--for', action='store', nargs='*',
                                metavar='simulation_name',
                                help='Specify a simulation (or multiple simulations) to run on')
    parser.add_argument('--backwards',action='store_true',
                                help='Process in reverse order (low-z first)')

    parser.add_argument('--part', action='store', nargs=2, type=int,
                                metavar=('N','M'),
                                help="Emulate MPI by handling slice N out of the total workload of M items. If absent, use real MPI.")

    parser.add_argument('--dmonly', action='store_true',
                                help='only match halos based on DM particles. Much more memory efficient. ONLY WORKS FOR ROCKSTAR HALOS')


    args = parser.parse_args()

    db.process_options(args)


    max_gp = args.hmax
    force = args.force
    dmonly = args.dmonly



    base_sim = db.sim_query_from_name_list(args.sims)


    pairs = []

    for x in base_sim:
        ts = db.core.get_default_session().query(halo_db.core.timestep.TimeStep).filter_by(
            simulation_id=x.id, available=True).order_by(halo_db.core.timestep.TimeStep.redshift.desc()).all()
        for a, b in zip(ts[:-1], ts[1:]):
            pairs.append((a, b))

    if args.backwards:
        pairs = pairs[::-1]



    assert len(pairs) != 0

    g_x = None

    if args.part:
        pair_list = parallel_tasks.distributed(pairs,args.part[0], args.part[1])
    else:
        pair_list = parallel_tasks.distributed(pairs)

    parallel_tasks.mpi_sync_db(session)

    session.commit()

    for s_x, s in pair_list:
        print s_x,"-->",s
        if force or crosslink.need_crosslink_ts(s_x, s):
            crosslink.crosslink_ts(s_x, s, 0, max_gp, dmonly)


parallel_tasks.launch(run)
