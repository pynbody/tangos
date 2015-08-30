#!/usr/bin/env python

import sqlalchemy
import time
import pynbody
import sys
import halo_db as db
import argparse
from halo_db import parallel_tasks
from terminalcontroller import term

session = db.internal_session



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
parser.add_argument('--part', action='store', nargs=2, type=int,
                            metavar=('N','M'),
                            help="Emulate MPI by handling slice N out of the total workload of M items. If absent, use real MPI.")


args = parser.parse_args()

db.process_options(args)


max_gp = args.hmax
force = args.force

print "Max groups per snapshot =",max_gp
if force:
    print "Forcing is ON"

try:
    base_sim = db.sim_query_from_name_list(args.sims)
    if base_sim != None:
        print "Running on: ", base_sim
        pairs = []

        for x in base_sim:
            ts = db.internal_session.query(db.TimeStep).filter_by(
                simulation_id=x.id, available=True).order_by(db.TimeStep.redshift.desc()).all()
            for a, b in zip(ts[:-1], ts[1:]):
                pairs.append((a, b))

except IndexError:
    simulation = []

assert len(pairs) != 0

g_x = None

pair_list = parallel_tasks.distributed(pairs,args.part[0], args.part[1])

parallel_tasks.mpi_sync_db(session)

time_dictionary = db.get_or_create_dictionary_item(session, "time")

session.commit()

for s_x, s in pair_list:

    print "Start process: ", s.filename[-6:], s_x.filename[-6:]

    targets = [x.id for x in session.query(
        db.Halo).filter_by(timestep=s_x).all()]
    # truncate targets list otherwise SQL can't cope. This is only for
    # checking whether there is SOMETHING there so should be fine.
    targets = targets[:100]

    if (not force) and session.query(db.HaloLink).filter(db.and_(db.HaloLink.halo_from_id.in_(targets), db.HaloLink.relation_id == time_dictionary.id)).count() > 0:
        print term.GREEN + " Existing link objects -- moving on! (Use force on command line to override)" + term.NORMAL
        continue

    f = pynbody.load(s.filename, only_header=True)

    f_x = pynbody.load(s_x.filename, only_header=True)

    try:
        f.halos()
        f_x.halos()
    except:
        print "Failure loading halo cat - continuing"
        continue

    try:
        min = 1
        max = len(f.halos())
        max2 = len(f_x.halos())
        max = max if max < max2 else max2
        max = max if max < max_gp else max_gp
        print "cat=", min, max
        if max < 2:
            continue

        if "no_iord" in sys.argv or len(f.gas) == 0 and len(f_x.gas) == 0:
            print "Briding WITHOUT iords"
            cat = pynbody.bridge.Bridge(f_x, f).match_catalog(min, max)
        else:
            cat = pynbody.bridge.OrderBridge(
                f_x, f).match_catalog(min, max)

        # cat = match_catalogue(g_x, g, siman.TipsyBridge(f_x, f), 1,40)

        for retries in xrange(10, 0, -1):
            try:

                # Delete existing entries before inserting new onews
                # direct query delete doesn't work for some reason,
                # probably something to do with in_ statement?
                dl_tab = session.query(db.HaloLink).filter(
                    db.HaloLink.halo_from_id.in_(targets)).all()

                if len(dl_tab) > 0:
                    print "Warning: deleting ", dl_tab, " entries"
                for dl in dl_tab:
                    session.delete(dl)

                for i in xrange(1, len(cat)):
                    print "...", i, "->", cat[i]
                    # record matching from g_x -> g
                    if cat[i] != -1:
                        a = session.query(db.Halo).filter(
                            db.and_(db.Halo.timestep == s_x, db.Halo.halo_number == i)).first()
                        b = session.query(db.Halo).filter(
                            db.and_(db.Halo.timestep == s, db.Halo.halo_number == int(cat[i]))).first()
                        if a != None and b != None:
                            cx = db.HaloLink(a, b, time_dictionary)
                            print "Create: ", cx
                            session.merge(cx)
                        else:
                            print "Missing halo database entry for ", s_x, i, s, i - 1
                    else:
                        print "No link for: ", session.query(db.Halo).filter(db.and_(db.Halo.timestep == s_x, db.Halo.halo_number == i)).first()
                session.commit()
                break

            except sqlalchemy.exc.OperationalError:
                if retries == 0:
                    raise
                session.rollback()
                print "DB is locked, retrying in 1 second (%d attempts remain)", retries
                time.sleep(1)
            else:
                raise RuntimeError, "Could not write to DB"

    except:
        session.rollback()
        raise

    f_x = f
    s_x = s
