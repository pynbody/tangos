"""SQLAlchemy Metadata and Session object"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

__all__ = ['Base', 'Session']

# SQLAlchemy session manager. Updated by model.init_model()
Session = scoped_session(sessionmaker())

# The declarative Base
Base = declarative_base()

from tangos.core import DictionaryItem, Simulation, TimeStep, Halo, HaloLink, HaloProperty
from tangos.core.stored_options import ArrayPlotOptions
from tangos.core.simulation import SimulationProperty
from tangos.core import Creator, Simulation, TimeStep, Halo, HaloProperty, HaloLink
from sqlalchemy import and_,or_,not_

