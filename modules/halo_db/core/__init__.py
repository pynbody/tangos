from sqlalchemy import Index, create_engine
from sqlalchemy import and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, clear_mappers

from .. import config

_verbose = False
internal_session=None
engine=None
Session=None



clear_mappers()  # remove existing maps

Base = declarative_base()


from .dictionary import DictionaryItem
from .creator import Creator
from .simulation import Simulation, SimulationProperty
from .tracking import TrackData, update_tracker_halos
from .timestep import TimeStep
from .halo import Halo
from .halo_data import HaloProperty, HaloLink
from .stored_options import ArrayPlotOptions

Index("halo_index", HaloProperty.__table__.c.halo_id)
Index("name_halo_index", HaloProperty.__table__.c.name_id,
      HaloProperty.__table__.c.halo_id)
Index("halo_timestep_index", Halo.__table__.c.timestep_id)
Index("halo_creator_index", Halo.__table__.c.creator_id)
Index("haloproperties_creator_index", HaloProperty.__table__.c.creator_id)
Index("halolink_index", HaloLink.__table__.c.halo_from_id)
Index("named_halolink_index", HaloLink.__table__.c.relation_id, HaloLink.__table__.c.halo_from_id)

def all_simulations(session=None):
    global internal_session
    if session is None:
        session = internal_session
    return session.query(Simulation).all()


def all_creators():
    return internal_session.query(Creator).all()


def sim_query_from_name_list(names, session=None):
    if session == None:
        session = internal_session

    query = session.query(Simulation)
    clause = None
    if names is not None:
        for rxi in names:
            if clause is None:
                clause = (Simulation.basename == rxi)
            else:
                clause |= (Simulation.basename == rxi)

        query = query.filter(clause)

    return query

def sim_query_from_args(argv, session=None):
    if session == None:
        session = internal_session

    query = session.query(Simulation)

    if "for" in argv:
        rx = argv[argv.index("for") + 1:]
        clause = None
        for rxi in rx:
            if clause is None:
                clause = (Simulation.basename == rxi)
            else:
                clause = clause | (Simulation.basename == rxi)

        query = query.filter(clause)

    return query


def get_simulation(id, session=None):
    if session is None:
        session = internal_session
    if isinstance(id, str):
        assert "/" not in id
        if "%" in id:
            match_clause = Simulation.basename.like(id)
        else:
            match_clause = Simulation.basename == id
        res = session.query(Simulation).filter(match_clause)
        num = res.count()
        if num == 0:
            raise RuntimeError, "No simulation matches %r" % id
        elif num > 1:
            raise RuntimeError, "Multiple (%d) matches for %r" % (num, id)
        else:
            return res.first()

    else:
        return session.query(Simulation).filter_by(id=id).first()







def get_timestep(id, session=None):
    if session is None:
        session = internal_session
    if isinstance(id, str):
        sim, ts = id.split("/")
        sim = get_simulation(sim)
        res = session.query(TimeStep).filter(
            and_(TimeStep.extension.like(ts), TimeStep.simulation_id == sim.id))
        num = res.count()
        if num == 0:
            raise RuntimeError, "No timestep matches for %r" % id
        elif num > 1:
            raise RuntimeError, "Multiple (%d) matches for timestep %r of simulation %r" % (
                num, ts, sim)
        else:
            return res.first()
    else:
        return session.query(TimeStep).filter_by(id=id).first()


def get_halo(id, session=None):
    """Get a halo from an ID or an identifying string

    Optionally, use the specified session.

    :rtype: Halo
    """
    if session is None:
        session = internal_session
    if isinstance(id, str):
        sim, ts, halo = id.split("/")
        ts = get_timestep(sim + "/" + ts)
        if "." in halo:
            halo_type, halo_number = map(int, halo.split("."))
        else:
            halo_type, halo_number = 0, int(halo)
        return session.query(Halo).filter_by(timestep_id=ts.id, halo_number=halo_number, halo_type=halo_type).first()
    return session.query(Halo).filter_by(id=id).first()


def get_item(path, session=None):
    c = path.count("/")
    if c is 0:
        return get_simulation(path, session)
    elif c is 1:
        return get_timestep(path, session)
    elif c is 2:
        return get_halo(path, session)


def get_haloproperty(id):
    return internal_session.query(HaloProperty).filter_by(id=id).first()

def get_items(path_list, session=None):
    return [get_item(path,session) for path in path_list]

def copy_property(halo_from, halo_to, *props):
    halo_from = get_halo(halo_from)
    try:
        halo_to = int(halo_to)
    except:
        pass
    if isinstance(halo_to, int):
        halo_to = get_halo(halo_to)
    elif "/" in halo_to:
        halo_to = get_halo(halo_to)
    else:
        halo_to = halo_from[halo_to]

    while halo_from is not None:
        for p in props:
            try:
                halo_to[p] = halo_from[p]
            except KeyError:
                pass
        halo_to = halo_to.next
        halo_from = halo_from.next

    internal_session.commit()


def getdb(cl) :
    """Function decorator to ensure input is parsed into a database object."""
    def getdb_inner(f) :
        def wrapped(*args, **kwargs) :

            if not isinstance(args[0], Base) :
                args = list(args)
                if isinstance(args[0], int) :
                    item = internal_session.query(cl).filter_by(id=args[0]).first()
                else :
                    item = get_item(args[0])
                if not isinstance(item, cl) :
                    if isinstance(item, Simulation) and cl is Halo:
                        print "Picking first timestep and first halo"
                        item = item.timesteps[0].halos[0]
                    else :
                        raise RuntimeError, "Path points to wrong type of db object %r"%item
                args[0] = item
            return f(*args,**kwargs)
        return wrapped
    return getdb_inner


def supplement_argparser(argparser):
    argparser.add_argument("--db-filename", help="Specify path to a database file to be used",
                           action='store', type=str, metavar="database_file.sqlite3")
    argparser.add_argument("--db-verbose", action='store_true',
                           help="Switch on sqlalchemy echo mode")


def process_options(argparser_options):
    global _verbose
    if argparser_options.db_filename is not None:
        config.db = argparser_options.db_filename
    _verbose = argparser_options.db_verbose

def init_db(db_uri=None, timeout=30, verbose=None):
    global _verbose, internal_session, engine, Session
    if db_uri is None:
        db_uri = 'sqlite:///' + config.db
    engine = create_engine(db_uri, echo=verbose or _verbose,
                           isolation_level='READ UNCOMMITTED',  connect_args={'timeout': timeout})
    Session = sessionmaker(bind=engine)
    internal_session=Session()
    Base.metadata.create_all(engine)

def get_engine():
    global engine
    return engine

from .dictionary import _get_dict_cache_for_session, get_dict_id, get_or_create_dictionary_item

__all__ = ['DictionaryItem', 'use_blocking_session',
           'all_simulations','all_creators', 'sim_query_from_name_list', 'sim_query_from_args',
           'get_simulation', 'get_timestep', 'get_halo', 'get_item',
           'get_haloproperty','copy_property','getdb', 'supplement_argparser',
           'update_tracker_halos',
           'process_options', 'init_db', 'Base', 'Creator']
