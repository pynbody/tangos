"""Tangos - the agile numerical galaxy organization system

This package provides ways to create and interpret databases of numerical galaxy formation simulations for scientific
analysis.

For information on getting started, see README.md.

"""

import sqlalchemy
import sqlalchemy.orm.session
from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Index,
                        Integer, String, and_, create_engine, or_, orm)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (backref, clear_mappers, deferred, relationship,
                            sessionmaker)
from sqlalchemy.orm.session import Session

from . import core, log, properties
from .core import *
from .query import *

__version__ = '1.8.0'
