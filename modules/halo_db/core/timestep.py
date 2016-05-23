import weakref
import os, os.path

from sqlalchemy import Column, Integer, String, ForeignKey, Float, Boolean, and_
from sqlalchemy.orm import relationship, backref

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

    def load(self):
        handler = self.simulation.get_output_set_handler()
        return handler.load_timestep(self.extension)

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

    @property
    def earliest(self):
        if self.previous is not None:
            return self.previous.earliest
        else:
            return self

    @property
    def latest(self):
        if self.next is not None:
            return self.next.latest
        else:
            return self

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
    def next(self):
        from . import Session

        try:
            return self._next
        except:
            session = Session.object_session(self)
            self._next = session.query(TimeStep).filter(and_(
                TimeStep.time_gyr > self.time_gyr, TimeStep.simulation == self.simulation)).order_by(TimeStep.time_gyr).first()
            return self._next

    @property
    def previous(self):
        from . import Session
        try:
            return self._previous
        except:
            session = Session.object_session(self)
            self._previous = session.query(TimeStep).filter(and_(
                TimeStep.time_gyr < self.time_gyr, TimeStep.simulation == self.simulation)).order_by(TimeStep.time_gyr.desc()).first()
            return self._previous