#!/usr/bin/env python2.7

import argparse
import sys
from textwrap import dedent

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import tangos as db
from tangos import all_simulations, config, core
from tangos.core import (Base, Creator, HaloLink, HaloProperty, Simulation,
                         SimulationObjectBase, TimeStep,
                         get_or_create_dictionary_item)
from tangos.core.simulation import SimulationProperty
from tangos.core.tracking import TrackData
from tangos.input_handlers import get_named_handler_class
from tangos.log import logger
from tangos.query import get_halo, get_simulation
from tangos.tools.add_simulation import SimulationAdderUpdater


def add_simulation_timesteps(options):
    handler=options.handler
    output_class = get_named_handler_class(handler).best_matching_handler(options.sim)
    output_object = output_class(options.sim)
    output_object.quicker = options.quicker
    adder = SimulationAdderUpdater(output_object,renumber=not options.no_renumber)
    adder.min_halo_particles = options.min_particles
    adder.max_num_objects = options.max_objects
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
    creator = Creator()
    ext_session.add(creator)
    core.set_creator(creator)

    _db_import_export(ext_session, int_session, *sims)

    core.set_creator(_xcurrent_creator)
    core.set_default_session(int_session)


def _db_import_export(target_session, from_session, *sims):
    external_id_to_internal_halo = {}
    translated_halolink_ids = []

    if len(sims)==0:
        sims = [x.id for x in all_simulations(from_session)]

    for sim in sims:
        ext_sim = get_simulation(sim, from_session)
        sim = Simulation(ext_sim.basename)
        target_session.add(sim)
        logger.info("Transferring simulation %s", ext_sim)

        halos_this_ts = []
        for p_ext in ext_sim.properties:
            dic = get_or_create_dictionary_item(
                target_session, p_ext.name.text)
            p = SimulationProperty(sim, dic, p_ext.data)
            halos_this_ts.append(p)

        for tk_ext in ext_sim.trackers:
            tk = TrackData(sim, tk_ext.halo_number)
            tk.particles = tk_ext.particles
            tk.use_iord = tk_ext.use_iord
            halos_this_ts.append(tk)

        target_session.add_all(halos_this_ts)

        for ts_ext in ext_sim.timesteps:
            logger.info("Transferring timestep %s",ts_ext)
            ts = TimeStep(sim, ts_ext.extension)
            ts.redshift = ts_ext.redshift
            ts.time_gyr = ts_ext.time_gyr
            ts.available = True
            target_session.add(ts)

            halos_this_ts = []

            logger.info("Transferring objects for %s", ts_ext)
            for h_ext in ts_ext.objects:
                h = SimulationObjectBase(ts, h_ext.halo_number, h_ext.finder_id, h_ext.finder_offset, h_ext.NDM,
                         h_ext.NStar, h_ext.NGas, h_ext.object_typecode)
                h.external_id = h_ext.id
                halos_this_ts.append(h)

            target_session.add_all(halos_this_ts)
            target_session.commit()

            for h in halos_this_ts:
                assert h.id is not None and h.id > 0
                external_id_to_internal_halo[h.external_id] = h

            logger.info("Transferring object properties for %s", ts_ext)
            for h_ext in ts_ext.objects:
                h_new = external_id_to_internal_halo[h_ext.id]
                for p_ext in h_ext.properties:
                    dic = get_or_create_dictionary_item(
                        target_session, p_ext.name.text)
                    dat = p_ext.data_raw
                    if dat is not None:
                        p = HaloProperty(h_new, dic, dat)
                        target_session.add(p)

            target_session.commit()

        for ts_ext in ext_sim.timesteps:
            logger.info("Transferring halolinks for timestep %s", ts_ext)
            sys.stdout.flush()
            _translate_halolinks(
                target_session, ts_ext.links_from, external_id_to_internal_halo, translated_halolink_ids)
            _translate_halolinks(
                target_session, ts_ext.links_to, external_id_to_internal_halo, translated_halolink_ids)
            target_session.commit()

        logger.info("Done")



def _translate_halolinks(target_session, halolinks, external_id_to_internal_halo, translated):
    for hl_ext in halolinks:
        if hl_ext.id in translated:
            continue

        dic = get_or_create_dictionary_item(
            target_session, hl_ext.relation.text)
        hl_new = HaloLink(external_id_to_internal_halo[hl_ext.halo_from_id],
                          external_id_to_internal_halo[hl_ext.halo_to_id],
                          dic)

        target_session.add(hl_new)

        translated.append(hl_ext.id)


def flag_duplicates_deprecated(opts):

    session = db.core.get_default_session()

    print("unmark all properties:", session.execute("update haloproperties set deprecated=0").rowcount)
    print("duplicate properties marked:", session.execute("update haloproperties set deprecated=1 where id in (SELECT min(id) FROM haloproperties GROUP BY halo_id, name_id HAVING COUNT(*)>1 ORDER BY halo_id, name_id)").rowcount)

    print("unmark all links:", session.execute("update halolink set deprecated=0").rowcount)
    print("duplicate links marked:", session.execute("update halolink set deprecated=1 where id in (SELECT min(id) FROM halolink GROUP BY halo_from_id, halo_to_id, relation_id HAVING COUNT(*)>1 ORDER BY halo_from_id, halo_to_id, weight)").rowcount)

    session.commit()

def remove_duplicates(options):

    session = db.core.get_default_session()

    # Note: the MySQL documentation states that “You cannot delete from a table and
    # select from the same table in a subquery.”. You can however circumvent this
    # limitation by creating an implicit temporary table.
    # See https://dev.mysql.com/doc/refman/5.6/en/delete.html
    # and https://stackoverflow.com/a/45498/2601223
    # Another approach would have been to use an inner join but unfortunately
    # SQLite does not support them in deletes, so we have to resort to this approach.
    # With MySQL 5.7.6 onwards, this implicit temporary tables may be optimized away
    # leading to the very same error we are trying to avoid. This can be fixed by
    # setting the optimizer_switch off for the query.
    # See https://dev.mysql.com/doc/relnotes/mysql/5.7/en/news-5-7-6.html#mysqld-5-7-6-optimizer
    dialect = session.connection().engine.dialect.dialect_description.split("+")[0].lower()
    if dialect == 'mysql':
        session.execute("SET @__optimizations = @@SESSION.optimizer_switch")
        session.execute("SET @@SESSION.optimizer_switch = 'derived_merge=off'")

    count = session.execute(dedent("""
        DELETE FROM haloproperties
        WHERE id NOT IN (
            SELECT * FROM (
                SELECT MAX(id)
                FROM haloproperties
                GROUP BY halo_id, name_id
            ) as t
        )
    """)).rowcount

    count_links = session.execute(dedent("""
        DELETE FROM halolink
        WHERE id NOT IN (
            SELECT * FROM (
                SELECT MAX(id)
                FROM halolink
                GROUP BY halo_from_id, halo_to_id, relation_id
            ) as t
        )
    """)).rowcount

    if dialect == 'mysql':
        session.execute("SET @@SESSION.optimizer_switch = @__optimizations")
    print("Deleted %d rows" % count)
    print("Deleted %d links" % count_links)
    session.commit()



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
    except Exception:

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
        except KeyError:
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
        except Exception:
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


def grep_run_id(opts):
    matching_runs = core.get_default_session().query(Creator).filter(
        Creator.command_line.like("%" + opts.query + "%")).all()
    for r in matching_runs:
        print(r.id, end=' ')
    print('\n')

def grep_run_info(opts):
    matching_runs = core.get_default_session().query(Creator).filter(
        Creator.command_line.like("%" + opts.query + "%")).all()
    for r in matching_runs:
        r.print_info()
    return matching_runs

def list_recent_runs(opts):
    n = opts.num
    run = core.get_default_session().query(Creator).order_by(
        Creator.id.desc()).limit(n).all()
    for r in run:
        r.print_info()


def _erase_run_content(run):
    run.halolinks.delete()
    run.halos.delete()
    run.properties.delete()
    run.timesteps.delete()
    for s in run.simulations:
        core.get_default_session().delete(s)
    core.get_default_session().commit()
    core.get_default_session().delete(run)
    core.get_default_session().commit()

def rem_run(id, confirm=True):
    run = core.get_default_session().query(Creator).filter_by(id=id).first()

    if run is None:
        raise ValueError(" Run %i does not exist" % id)

    print("You want to delete everything created by the following run:")
    run.print_info()

    if confirm:
        print(""">>> type "yes" to continue""")

    if (not confirm) or input(":").lower() == "yes":
        _erase_run_content(run)
        print("OK")
    else:
        print("aborted")

def grep_remove_runs(opts):
    print('Will remove the following run(s): ')
    matching_runs = grep_run_info(opts)

    # Always ask for confirmation, as this action could be very destructive
    print("You want to delete everything created by the above run(s)?")
    print(""">>> type "yes" to continue""")

    if input(":").lower() == "yes":
        for r, run in enumerate(matching_runs):
            print('Removing run ',r+1,' of ',len(matching_runs))
            _erase_run_content(run)
        print("Done")
    else:
        print("aborted")


def rollback(options):
    if len(options.ids)>0:
        for run_id in options.ids:
            rem_run(run_id, not options.force)
    else:
        most_recent_id = core.get_default_session().query(Creator).order_by(
          Creator.id.desc()).first().id
        rem_run(most_recent_id, not options.force)

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

    longest_class_name = max(len(format_class_name(cl)) for cl in properties.all_property_classes())
    print("{} | {} | {}".format("name".rjust(30), "handler".center(15), "property class"))
    print("-"*30+"-+-"+"-"*15+"-+-"+"-"*longest_class_name)
    for p in all_properties:
        classes = properties.all_providing_classes(p)
        print("%s | %.15s | %s"%(p.rjust(30), format_handler_name(classes[0]), format_class_name(classes[0])))
        for additional_class in classes[1:]:
            print(" "*30+" | %.15s | %s"%(format_handler_name(additional_class),
                                          format_class_name(additional_class)))

def diff(options):
    from ..testing import db_diff
    differ = db_diff.TangosDbDiff(options.uri1, options.uri2, ignore_keys=options.ignore_value_of)
    if options.simulation:
        differ.compare_simulation(options.simulation)
    elif options.timestep:
        differ.compare_timestep(options.timestep)
    elif options.object:
        differ.compare_object(options.object)
    else:
        differ.compare()
    result = differ.failed

    if result:
        logger.info("Differences found. Exiting with status 1.")
        sys.exit(1)
    else:
        logger.info("No differences found.")


def main():
    print("""
    The 'tangos_manager' command line is deprecated in favour of just 'tangos'.
    'tangos_manager' may be removed in future versions.
    """)

    parser, _ = get_argument_parser_and_subparsers()

    args = parser.parse_args()
    core.process_options(args)
    args.func(args)


def get_argument_parser_and_subparsers():
    parser = argparse.ArgumentParser()
    core.supplement_argparser(parser)
    subparse = parser.add_subparsers()

    subparse_add = subparse.add_parser("add",
                                       help="Add new simulations to the database, or update existing simulations")
    subparse_add.add_argument("sim", action="store",
                              help="The path to the simulation folders relative to the database folder")
    subparse_add.add_argument("--handler", action="store",
                              help="The handler to use from the simulation_outputs subpackage",
                              default=config.default_fileset_handler_class)
    subparse_add.add_argument("--min-particles", action="store", type=int, default=config.min_halo_particles,
                              help="The minimum number of particles a halo must have before it is imported (default %d)" % config.min_halo_particles)
    subparse_add.add_argument("--max-objects", action="store", type=int, default=config.max_num_objects,
                              help="The maximum number of objects of a particular type to store (no limit if not specified)")
    subparse_add.add_argument("--quicker", action="store_true",
                              help="Cut corners/make guesses to import quickly and with minimum memory usage. Only use if you understand the consequences!")
    subparse_add.add_argument("--no-renumber", action="store_true",
                              help="By default tangos renumbers halos to start from 1, in decreasing order of dark matter particles. Set this flag to keep the original halo finder numbers.")
    subparse_add.set_defaults(func=add_simulation_timesteps)

    subparse_recentruns = subparse.add_parser("recent-runs",
                                              help="List information about the most recent database updates")
    subparse_recentruns.set_defaults(func=list_recent_runs)
    subparse_recentruns.add_argument("num", type=int,
                                     help="The number of runs to display, starting with the most recent")

    subparse_greprun = subparse.add_parser("grep-run-ids",
                                          help="List IDs of runs matching command line input")
    subparse_greprun.set_defaults(func=grep_run_id)
    subparse_greprun.add_argument("query", type=str,
                                     help="The sub-string to search for in the command line")

    subparse_grepruninfo = subparse.add_parser("grep-runs",
                                          help="List details of runs matching command line input")
    subparse_grepruninfo.set_defaults(func=grep_run_info)
    subparse_grepruninfo.add_argument("query", type=str,
                                     help="The sub-string to search for in the command line")

    subparse_grepremove = subparse.add_parser("grep-remove",
                                           help="Remove runs matching command line input")
    subparse_grepremove.set_defaults(func=grep_remove_runs)
    subparse_grepremove.add_argument("query", type=str,
                                      help="The sub-string to search for in the command line")


    # The following subcommands currently do not work and is disabled:
    """
    subparse_remruns = subparse.add_parser("rm", help="Remove a simulation from the database")
    subparse_remruns.add_argument("sims", help="The path to the simulation folder relative to the database folder")
    subparse_remruns.set_defaults(func=rem_simulation_timesteps)
     """

    subparse_import = subparse.add_parser("import",
                                          help="Import one or more simulations from another sqlite file")
    subparse_import.add_argument("file", type=str, help="The filename of the sqlite file from which to import")
    subparse_import.add_argument("sims", nargs="*", type=str,
                                 help="The name of the simulations to import (or import everything if none specified)")
    subparse_import.set_defaults(func=db_import)



    subparse_deprecate = subparse.add_parser("flag-duplicates",
                                             help="Flag old copies of properties and duplicate links (if they are present)")
    subparse_deprecate.set_defaults(func=flag_duplicates_deprecated)
    subparse_deprecate = subparse.add_parser("remove-duplicates",
                                             help="Remove old copies of properties and duplicate links (if they are present)")
    subparse_deprecate.set_defaults(func=remove_duplicates)

    subparse_rollback = subparse.add_parser("rollback", help="Remove database updates")
    subparse_rollback.add_argument("ids", nargs="*", type=int, help="IDs of the database updates to remove. If none specified, removes the most recent run.")
    subparse_rollback.add_argument("--force", "-f", action="store_true", help="If this flag is present, no confirmation prompts will be issued")
    subparse_rollback.set_defaults(func=rollback)

    subparse_dump_id = subparse.add_parser("dump-iord", help="Dump the iords corresponding to a specified halo")
    subparse_dump_id.add_argument("halo", type=str, help="The identity of the halo to dump")
    subparse_dump_id.add_argument("filename", type=str, help="A filename for the output text file")
    subparse_dump_id.add_argument("size", type=str, nargs="?",
                                  help="Size, in kpc, of sphere to extract (or omit to get just the halo particles)")
    subparse_dump_id.add_argument("family", type=str, help="The family of particles to extract", default="")
    subparse_dump_id.set_defaults(func=dump_id)


    subparse_diff = subparse.add_parser("diff", help="Analyse the difference between two databases")
    subparse_diff.add_argument("uri1", type=str, help="The first database URI or filename")
    subparse_diff.add_argument("uri2", type=str, help="The second database URI or filename")
    subparse_diff.add_argument("--simulation", type=str, help="Only compare the specified simulation", default=None)
    subparse_diff.add_argument("--timestep", type=str, help="Only compare the specified timestep", default=None)
    subparse_diff.add_argument("--object", type=str, help="Only compare the specified object", default=None)
    subparse_diff.add_argument("--ignore-value-of", nargs="*", type=str, help="Ignore the value of the specified properties", default=None)
    subparse_diff.set_defaults(func=diff)

    subparse_list_available_properties = subparse.add_parser("list-possible-properties",
                                                             help="List all the object properties that can be calculated by the currently available modules")
    subparse_list_available_properties.set_defaults(func=list_available_properties)
    return parser, subparse
