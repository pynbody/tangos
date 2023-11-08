import os

import sqlalchemy
from sqlalchemy import Index, create_engine, event, inspect, text
from sqlalchemy.orm import clear_mappers, declarative_base, sessionmaker

from .. import config, log

_verbose = False
_internal_session=None
_internal_session_args=None
_internal_session_pid=None
_engine=None
Session=None


def get_default_session() -> sqlalchemy.orm.Session:
    """Get the default ORM session to be used when no other is specified."""
    global _internal_session, _internal_session_pid, _internal_session_args
    if _internal_session is None:
        init_db()
    elif _internal_session_pid!=os.getpid():
        if _internal_session_args is None:
            raise RuntimeError("The process has been forked, but no information is available to open a new connection to the database")
        else:
            init_db(*_internal_session_args)
    return _internal_session


def set_default_session(session):
    """Set the default ORM session to be used.

    No validation is carried out. If an inappropriate session is passed in, many database functions
    will subsequently fail. """
    global _internal_session
    _internal_session = session
    _internal_session_pid = os.getpid()
    _internal_session_args = None


def get_default_engine() -> sqlalchemy.engine.Engine:
    """Get the default sqlalchemy engine to be used when no other is specified."""
    global _engine
    if _engine is None:
        init_db()
    return _engine



clear_mappers()  # remove existing maps

Base = declarative_base()


from .creator import Creator
from .dictionary import DictionaryItem
from .halo import SimulationObjectBase
from .halo_data import HaloLink, HaloProperty
from .simulation import Simulation, SimulationProperty
from .timestep import TimeStep
from .tracking import TrackData, update_tracker_halos

Index("halo_index", HaloProperty.__table__.c.halo_id)
Index("name_halo_index", HaloProperty.__table__.c.name_id,
      HaloProperty.__table__.c.halo_id)
Index("halo_timestep_index", SimulationObjectBase.__table__.c.timestep_id)
Index("halo_creator_index", SimulationObjectBase.__table__.c.creator_id)
Index("halo_finder_index", SimulationObjectBase.__table__.c.finder_id)
Index("haloproperties_creator_index", HaloProperty.__table__.c.creator_id)
Index("halolink_index", HaloLink.__table__.c.halo_from_id)
Index("halolink_bidirectional_index", HaloLink.__table__.c.halo_to_id, HaloLink.__table__.c.halo_from_id)
Index("named_halolink_index", HaloLink.__table__.c.relation_id, HaloLink.__table__.c.halo_from_id)




def sim_query_from_name_list(names, session=None):
    if session is None:
        session = get_default_session()

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
    if session is None:
        session = get_default_session()

    query = session.query(Simulation)

    if "--sims" in argv:
        rx = argv[argv.index("--sims") + 1:]
        clause = None
        for rxi in rx:
            if clause is None:
                clause = (Simulation.basename == rxi)
            else:
                clause = clause | (Simulation.basename == rxi)

        query = query.filter(clause)

    return query


def supplement_argparser(argparser):
    argparser.add_argument("--db-filename", "-d", help="Specify path to a database file to be used",
                           action='store', type=str, metavar="database_file.sqlite3")
    argparser.add_argument("--db-verbose", "-v", action='store_true',
                           help="Switch on sqlalchemy echo mode")


def process_options(argparser_options):
    global _verbose
    if argparser_options.db_filename is not None:
        config.db = argparser_options.db_filename
    _verbose = argparser_options.db_verbose

def _check_and_upgrade_database(engine, colname='finder_offset'):
    inspector = inspect(engine)
    if 'halos' in inspector.get_table_names():
        cols = inspector.get_columns('halos')
        for c in cols:
            if c['name']==colname:
                return
        log.logger.warning("The database uses an old schema, missing the finder_offset column from halos. Attempting to update.")
        with engine.begin() as connection:
            connection.execute(text(f"alter table halos add column {colname} integer;"))
            connection.execute(text(f"update halos set {colname} = finder_id;"))
        log.logger.warning("The database update appeared to complete without any problems.")


def init_db(db_uri=None, timeout=30, verbose=None):
    global _verbose, _internal_session, _engine, Session, _internal_session_args, _internal_session_pid
    if db_uri is None:
        db_uri = config.db

    if 'psycopg2' in db_uri:
        from ..util import postgresql_adapters
        postgresql_adapters.register_postgresql_adapters()

    if '//' not in db_uri:
        db_uri = 'sqlite:///' + db_uri

    if db_uri.startswith("sqlite:///"):
        connect_args = {"timeout": timeout}
    else:
        connect_args = {"connect_timeout": timeout}

    _engine = create_engine(
        db_uri,
        echo=verbose or _verbose,
        isolation_level='READ UNCOMMITTED',
        connect_args=connect_args,
        future=True
    )

    _check_and_upgrade_database(_engine)

    Session = sessionmaker(bind=_engine, future=True)
    _internal_session=Session()
    Base.metadata.create_all(_engine)
    creator.set_creator(None)
    _internal_session_args = (db_uri, timeout, verbose)
    _internal_session_pid = os.getpid() # stored so that we can detect when a fork happens

def close_db():
    global _engine
    close_session()
    if _engine is not None:
        _engine.dispose()
        _engine = None

def close_session():
    global Session, _internal_session
    if _internal_session is not None:
        _internal_session.close()
        _internal_session = None
    if Session is not None:
        Session = None

from .dictionary import (_get_dict_cache_for_session, get_dict_id,
                         get_or_create_dictionary_item)

__all__ = ['DictionaryItem',
           'sim_query_from_name_list', 'sim_query_from_args',
           'supplement_argparser',
           'update_tracker_halos',
           'process_options', 'init_db', 'Base', 'Creator',
           'get_default_session']
