"""SQLAlchemy Metadata and Session object"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

__all__ = ['Base', 'Session']

# SQLAlchemy session manager. Updated by model.init_model()
Session = scoped_session(sessionmaker())

# The declarative Base
Base = declarative_base()

from halo_db import DictionaryItem, Creator, Simulation, TimeStep, Halo, HaloLink, HaloProperty, ArrayPlotOptions, SimulationProperty
from sqlalchemy import and_,or_,not_

