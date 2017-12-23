#!/usr/bin/env python2.7

from __future__ import absolute_import
from __future__ import print_function
import sys

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import tangos as db
from tangos import all_simulations
from tangos import core, config
from tangos.core import Base, get_or_create_dictionary_item, \
    Creator, Simulation, TimeStep, Halo, HaloProperty, HaloLink
from tangos.core.simulation import SimulationProperty
from tangos.core.tracking import TrackData
from tangos.query import get_simulation, get_halo
from tangos.simulation_output_handlers import get_named_handler_class
from tangos.tools.add_simulation import SimulationAdderUpdater
from six.moves import input


def add_simulation_timesteps(options):
    handler=options.handler
    output_class = get_named_handler_class(handler).best_matching_handler(options.sim)
    output_object = output_class(options.sim)
    output_object.quicker = options.quicker
    adder = SimulationAdderUpdater(output_object)
    adder.min_halo_particles = options.min_particles
    adder.scan_simulation_and_add_all_descendants()




def db_import(options):

    sims = options.sims
    remote_db = options.file

    global internal_session
    engine2 = create_engine('sqlite:///' + remote_db, echo=False)
    ext_session = sessionmaker(bind=engine2)()

    _db_import_export(core.get_default_session(), ext_session, *sims)


def db_export(remote_db, *sims):

    global internal_session
    engine2 = create_engine('sqlite:///' + remote_db, echo=False)

    int_session = core.get_default_session()
    ext_session = sessionmaker(bind=engine2)()

    Base.metadata.create_all(engine2)

    _xcurrent_creator = core.creator.get_creator()

    core.set_default_session(ext_session)
    core.set_creator(ext_session.merge(Creator()))

    _db_import_export(ext_session, int_session, *sims)

    core.set_creator(_xcurrent_creator)
    core.set_default_session(int_session)


def _db_import_export(target_session, from_session, *sims):
    from tangos.util.terminalcontroller import heading
    external_to_internal_halo_id = {}
    translated_halolink_ids = []

    if sims == tuple():
        sims = [x.id for x in all_simulations(from_session)]

    for sim in sims:
        ext_sim = get_simulation(sim, from_session)
        sim = Simulation(ext_sim.basename)
        heading("import " + repr(ext_sim))
        sim = target_session.merge(sim)

        for p_ext in ext_sim.properties:
            dic = get_or_create_dictionary_item(
                target_session, p_ext.name.text)
            p = SimulationProperty(sim, dic, p_ext.data)
            p = target_session.merge(p)

        for tk_ext in ext_sim.trackers:
            tk = TrackData(sim, tk_ext.halo_number)
            tk.particles = tk_ext.particles
            tk.use_iord = tk_ext.use_iord

        for ts_ext in ext_sim.timesteps:
            print(".", end=' ')
            sys.stdout.flush()
            ts = TimeStep(sim, ts_ext.extension, False)
            ts.redshift = ts_ext.redshift
            ts.time_gyr = ts_ext.time_gyr
            ts.available = True
            ts = target_session.merge(ts)
            for h_ext in ts_ext.halos:
                h = Halo(ts, h_ext.halo_number, h_ext.finder_id, h_ext.NDM,
                         h_ext.NStar, h_ext.NGas, h_ext.object_typecode)
                h = target_session.merge(h)
                assert h.id is not None and h.id > 0
                external_to_internal_halo_id[h_ext.id] = h.id
                for p_ext in h_ext.properties:
                    dic = get_or_create_dictionary_item(
                        target_session, p_ext.name.text)
                    dat = p_ext.data
                    if dat is not None:
                        p = HaloProperty(h, dic, dat)
                        p = target_session.merge(p)

        print("Translate halolinks", end=' ')
        for ts_ext in ext_sim.timesteps:
            print(".", end=' ')
            sys.stdout.flush()
            _translate_halolinks(
                target_session, ts_ext.links_from, external_to_internal_halo_id, translated_halolink_ids)
            _translate_halolinks(
                target_session, ts_ext.links_to, external_to_internal_halo_id, translated_halolink_ids)

        print("Done")
        target_session.commit()


def _translate_halolinks(target_session, halolinks, external_to_internal_halo_id, translated):
    for hl_ext in halolinks:
        if hl_ext.id in translated:
            continue

        dic = get_or_create_dictionary_item(
            target_session, hl_ext.relation.text)
        hl_new = HaloLink(None, None, dic)

        try:
            hl_new.halo_from_id = external_to_internal_halo_id[
                hl_ext.halo_from_id]
        except KeyError:
            continue

        try:
            hl_new.halo_to_id = external_to_internal_halo_id[hl_ext.halo_to_id]
        except KeyError:
            continue
        target_session.add(hl_new)
        translated.append(hl_ext.id)


def flag_duplicates_deprecated(opts):

    session = db.core.get_default_session()

    print("unmark all:",session.execute("update haloproperties set deprecated=0").rowcount)
    print("      mark:",session.execute("update haloproperties set deprecated=1 where id in (SELECT min(id) FROM haloproperties GROUP BY halo_id, name_id HAVING COUNT(*)>1 ORDER BY halo_id, name_id)").rowcount)

    session.commit()

def remove_duplicates(opts):
    flag_duplicates_deprecated(None)
    count = 1
    while count>0:
        # Order of name_id, halo_id in group clause below is important for optimisation - halo_id, name_id
        # does *not* match the index.
        count = db.core.get_default_session().execute("delete from haloproperties where haloproperties.id in "
                                                 "(SELECT min(id) FROM haloproperties "
                                                 "    GROUP BY name_id, halo_id HAVING COUNT(halo_id)>1);").rowcount
        if count>0 :
            print("Deleted %d rows"%count)
            print("  checking for further duplicates...")
    print("Done")
    db.core.get_default_session().commit()



def rem_simulation_timesteps(options):
    basename = options.sims
    from tangos.util.terminalcontroller import term

    sim = core.get_default_session().query(Simulation).filter_by(
        basename=basename).first()

    if sim == None:
        print(term.GREEN + "Simulation does not exist", term.NORMAL)
    else:
        print(term.RED + "Delete simulation", sim, term.NORMAL)
        core.get_default_session().delete(sim)


def add_tracker(halo, size=None):

    import pynbody

    try:
        halo = get_halo(halo)
    except:

        sim = get_simulation(halo)
        print("Adding tracker for isolated run", sim)
        halo = None

    if halo is not None:
        # check we can open the tracker file
        hfile = halo.load()
        hfile.physical_units()
        use_iord = True
        try:
            hfile.dm['iord']
        except:
            use_iord = False

        # get the centre

        cen = halo.get('SSC', None)
        if cen is None:
            cen = pynbody.analysis.halo.shrink_sphere_center(hfile.dm)

        hfile.ancestor.dm['pos'] -= cen

        if size is None:
            size = '500 pc'

        size = pynbody.units.Unit(size)
        try:
            size.in_units("kpc")
            X = hfile.ancestor.dm[pynbody.filt.Sphere(size)]
        except:
            size.in_units("kpc km s^-1")
            X = hfile.ancestor.dm[pynbody.filt.LowPass("j2", size ** 2)]
            size = str(size.in_units("kpc km s^-1")) + "_kks"

        if len(X) < 2:
            print("Alert! Track data is too short")
            import pdb
            pdb.set_trace()
        # setup the tracker
        tx = TrackData(halo.timestep.simulation)
        print("Tracker halo ID is", tx.halo_number)
        print("Length of track data is", len(X))
        tx.select(X, use_iord)
        ts_trigger = halo.timestep
    else:
        f = sim.timesteps[0].load()
        tx = TrackData(sim)
        if tx.halo_number != 1:
            print("Already have a tracker for this simulation -> abort")
            return
        print("Tracker halo ID is", tx.halo_number)
        tx.particles = np.array(
            np.arange(0, len(f.dm) - 1, 1), dtype=int)
        tx.use_iord = False
        ts_trigger = None

    core.get_default_session().add(tx)
    tx.create_objects(first_timestep=ts_trigger)
    if halo is not None:
        targ = halo.timestep.halos.filter_by(
            object_typecode=1, halo_number=tx.halo_number).first()
        size = str(size).replace(" ", "")
        halo["dm_central_" + size] = targ
        print("Copying SSC...", end=' ')
        sys.stdout.flush()
        while halo is not None:
            try:
                targ['SSC'] = halo['SSC']
            except KeyError:
                pass
            halo = halo.next
            targ = targ.next
        print("done")
    core.get_default_session().commit()


def grep_run(st):
    run = core.get_default_session().query(Creator).filter(
        Creator.command_line.like("%" + st + "%")).all()
    for r in run:
        print(r.id, end=' ')


def list_recent_runs(opts):
    n = opts.num
    run = core.get_default_session().query(Creator).order_by(
        Creator.id.desc()).limit(n).all()
    for r in run:
        r.print_info()


def rem_run(id, confirm=True):
    run = core.get_default_session().query(Creator).filter_by(id=id).first()
    print("You want to delete everything created by the following run:")
    run.print_info()

    if confirm:
        print(""">>> type "yes" to continue""")

    if (not confirm) or input(":").lower() == "yes":
        #for y in run.halolinks:
        #    core.get_default_session().delete(y)
        run.halolinks.delete()
        run.halos.delete()
        run.properties.delete()
        run.timesteps.delete()
        for s in run.simulations:
            print(s)
            core.get_default_session().delete(s)
        core.get_default_session().commit()
        core.get_default_session().delete(run)
        core.get_default_session().commit()
        print("OK")
    else:
        print("aborted")

def rollback(options):
    for run_id in options.ids:
        rem_run(run_id, not options.force)

def dump_id(options):
    import pynbody

    h = db.get_halo(options.halo).load()

    if options.size:
        pynbody.analysis.halo.center(h,vel=False)
        print("Size=",options.size)
        h = h.ancestor[pynbody.filt.Sphere(str(options.size)+" kpc")]

    if options.family!="":
        h = getattr(h,options.family)

    np.savetxt(options.filename,h['iord'],"%d")


def list_available_properties(options):
    from .. import properties
    all_properties = sorted(properties.all_properties())

    def format_class_name(cl):
        return "%s.%s"%(cl.__module__, cl.__name__)

    def format_handler_name(cl):
        if cl.requires_particle_data:
            return cl.works_with_handler.__name__.center(15)
        else:
            return "live".center(15)

    longest_class_name = max([len(format_class_name(cl)) for cl in properties.all_property_classes()])
    print("%s | %s | %s" % ("name".rjust(30), "handler".center(15), "property class"))
    print("-"*30+"-+-"+"-"*15+"-+-"+"-"*longest_class_name)
    for p in all_properties:
        classes = properties.all_providing_classes(p)
        print("%s | %.15s | %s"%(p.rjust(30), format_handler_name(classes[0]), format_class_name(classes[0])))
        for additional_class in classes[1:]:
            print(" "*30+" | %.15s | %s"%(format_handler_name(additional_class),
                                          format_class_name(additional_class)))

def main():

    #db.core.get_default_session() = tangos.blocking_session.BlockingSession(bind = db.core.engine)
    import argparse

    parser = argparse.ArgumentParser()
    core.supplement_argparser(parser)
    parser.add_argument("--verbose", action="store_true",
                        help="Print extra information")


    subparse = parser.add_subparsers()

    subparse_add = subparse.add_parser("add",
                                       help="Add new simulations to the database, or update existing simulations")
    subparse_add.add_argument("sim",action="store",
                              help="The path to the simulation folders relative to the database folder")
    subparse_add.add_argument("--handler", action="store",
                              help="The handler to use from the simulation_outputs subpackage",
                              default=config.default_fileset_handler_class)
    subparse_add.add_argument("--min-particles", action="store", type=int, default=config.min_halo_particles,
                              help="The minimum number of particles a halo must have before it is imported (default %d)"%config.min_halo_particles)
    subparse_add.add_argument("--quicker", action="store_true",
                              help="Cut corners/make guesses to import quickly and with minimum memory usage. Only use if you understand the consequences!")

    subparse_add.set_defaults(func=add_simulation_timesteps)

    subparse_recentruns = subparse.add_parser("recent-runs",
                                              help="List information about the most recent database updates")
    subparse_recentruns.set_defaults(func=list_recent_runs)
    subparse_recentruns.add_argument("num",type=int,
                                     help="The number of runs to display, starting with the most recent")

    subparse_remruns = subparse.add_parser("rm", help="Remove a simulation from the database")
    subparse_remruns.add_argument("sims",help="The path to the simulation folder relative to the database folder")
    subparse_remruns.set_defaults(func=rem_simulation_timesteps)

    subparse_deprecate = subparse.add_parser("flag-duplicates",
                                             help="Flag old copies of properties (if they are present)")
    subparse_deprecate.set_defaults(func=flag_duplicates_deprecated)

    subparse_deprecate = subparse.add_parser("remove-duplicates",
                                             help="Remove old copies of properties (if they are present)")
    subparse_deprecate.set_defaults(func=remove_duplicates)

    subparse_import = subparse.add_parser("import",
                                          help="Import one or more simulations from another sqlite file")
    subparse_import.add_argument("file",type=str,help="The filename of the sqlite file from which to import")
    subparse_import.add_argument("sims",nargs="*",type=str,help="The name of the simulations to import (or import everything if none specified)")
    subparse_import.set_defaults(func=db_import)

    subparse_rollback = subparse.add_parser("rollback", help="Remove database updates (by ID - see recent-runs)")
    subparse_rollback.add_argument("ids",nargs="*",type=int,help="IDs of the database updates to remove")
    subparse_rollback.add_argument("--force","-f",action="store_true",help="Do not prompt for confirmation")
    subparse_rollback.set_defaults(func=rollback)

    subparse_dump_id = subparse.add_parser("dump-iord", help="Dump the iords corresponding to a specified halo")
    subparse_dump_id.add_argument("halo",type=str,help="The identity of the halo to dump")
    subparse_dump_id.add_argument("filename",type=str,help="A filename for the output text file")
    subparse_dump_id.add_argument("size",type=str,nargs="?",help="Size, in kpc, of sphere to extract (or omit to get just the halo particles)")
    subparse_dump_id.add_argument("family",type=str,help="The family of particles to extract",default="")

    subparse_dump_id.set_defaults(func=dump_id)

    subparse_list_available_properties = subparse.add_parser("list-possible-properties", help = "List all the object properties that can be calculated by the currently available modules")
    subparse_list_available_properties.set_defaults(func=list_available_properties)


    args = parser.parse_args()
    core.process_options(args)
    core.init_db()
    args.func(args)
