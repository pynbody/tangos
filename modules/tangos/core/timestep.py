import weakref
import os, os.path

from sqlalchemy import Column, Integer, String, ForeignKey, Float, Boolean, and_
from sqlalchemy.orm import relationship, backref, aliased

from . import Base
from .creator import Creator
from .simulation import Simulation
from .. import config



class TimeStep(Base):
    __tablename__ = 'timesteps'

    id = Column(Integer, primary_key=True)
    extension = Column(String)
    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    redshift = Column(Float)
    time_gyr = Column(Float)
    creator = relationship(Creator, backref=backref(
        'timesteps', cascade='delete', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    available = Column(Boolean, default=True)

    simulation = relationship(Simulation, backref=backref('timesteps', order_by=time_gyr,
                                                          cascade='delete, merge'),
                              cascade='save-update, merge')

    @property
    def filename(self):
        return os.path.join(config.base, self.simulation.basename, self.extension)

    @property
    def relative_filename(self):
        return str(self.simulation.basename + "/" + self.extension)

    def load(self, mode=None):
        handler = self.simulation.get_output_set_handler()
        return handler.load_timestep(self.extension, mode=mode)

    def load_region(self, region_specification, mode=None):
        handler = self.simulation.get_output_set_handler()
        return handler.load_region(self.extension, region_specification, mode=mode)

    def __init__(self, simulation, extension):
        from . import creator
        self.extension = str(extension)
        self.simulation = simulation
        self.creator_id = creator.get_creator_id()

    def __repr__(self):
        extra = ""
        if not self.available:
            extra += " unavailable"
        path = self.path
        if self.redshift is None:
            return "<TimeStep %r%s>"%(path,extra)
        else:
            return "<TimeStep %r z=%.2f t=%.2f Gyr%s>" % (path, self.redshift, self.time_gyr, extra)

    def short(self):
        return "<TimeStep(... z=%.2f ...)>" % self.redshift

    def __getitem__(self, i):
        return self.halos[i]


    @property
    def path(self):
        return self.simulation.path+"/"+self.extension

    @property
    def redshift_cascade(self):
        a = self.next
        if a is not None:
            return [self.redshift] + a.redshift_cascade
        else:
            return [self.redshift]

    @property
    def time_gyr_cascade(self):
        a = self.next
        if a is not None:
            return [self.time_gyr] + a.time_gyr_cascade
        else:
            return [self.time_gyr]

    def keys(self):
        """Return keys for which ALL halos have a data entry"""
        from . import Session
        session = Session.object_session(self)
        raise RuntimeError, "Not implemented"

    def gather_property(self, *plist, **kwargs):
        """Gather up the specified properties from the child
        halos."""
        from .. import live_calculation
        from . import Session
        from .halo import Halo

        if isinstance(plist[0], live_calculation.Calculation):
            property_description = plist[0]
        else:
            property_description = live_calculation.parser.parse_property_names(*plist)

        # must be performed in its own session as we intentionally load in a lot of
        # objects with incomplete lazy-loaded properties
        session = Session()
        raw_query = session.query(Halo).filter_by(timestep_id=self.id)

        query = property_description.supplement_halo_query(raw_query)
        results = query.all()
        return property_description.values_sanitized(results)

    @property
    def earliest(self):
        return self.get_final(-1)

    @property
    def latest(self):
        return self.get_final(+1)

    @property
    def next(self):
        try:
            return self._next
        except AttributeError:
            self._next = self.get_next()
            return self._next

    @property
    def previous(self):
        try:
            return self._previous
        except AttributeError:
            self._previous = self.get_next(-1)
            return self._previous

    def get_next(self, steps=1):
        """Returns the next timestep, or its successor after the specified number of steps.

        If steps is negative, finds the previous timestep or predecessor by specified number of steps.

        If no such step exists, returns None."""

        if steps==0:
            return self
        from . import Session
        session = Session.object_session(self)

        if steps>0:
            direction_comparison = TimeStep.time_gyr > self.time_gyr
            first_order = TimeStep.time_gyr
        else:
            direction_comparison = TimeStep.time_gyr < self.time_gyr
            first_order = TimeStep.time_gyr.desc()

        successors_query = session.query(TimeStep).filter(
               and_(direction_comparison, TimeStep.simulation == self.simulation)
            ).order_by(first_order).limit(abs(steps))

        if successors_query.count()<abs(steps):
            return None

        successors_subquery = aliased(TimeStep, alias=successors_query.subquery())

        if steps>0:
            second_order = successors_subquery.time_gyr.desc()
        else:
            second_order = successors_subquery.time_gyr

        next = session.query(successors_subquery).order_by(second_order).first()

        return next

    def get_final(self, direction=1):
        """Returns the final timestep of this simulation, either the latest (direction=+1) or first (direction=-1)"""

        assert direction==1 or direction==-1

        from . import Session
        session = Session.object_session(self)

        q = session.query(TimeStep).filter_by(simulation_id=self.simulation_id)

        if direction==-1:
            q = q.order_by(TimeStep.time_gyr)
        else:
            q = q.order_by(TimeStep.time_gyr.desc())

        return q.first()


