import os
import os.path

from sqlalchemy import Boolean, Column, ForeignKey, Integer, Text, and_
from sqlalchemy.orm import Session, aliased, backref, relationship

from .. import config
from ..config import DOUBLE_PRECISION
from . import Base
from .creator import Creator
from .simulation import Simulation


class TimeStep(Base):
    __tablename__ = 'timesteps'

    id = Column(Integer, primary_key=True)
    extension = Column(Text)
    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    redshift = Column(DOUBLE_PRECISION)
    time_gyr = Column(DOUBLE_PRECISION)
    creator = relationship(Creator, backref=backref(
        'timesteps', cascade_backrefs=False, lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    available = Column(Boolean, default=True)

    simulation = relationship(Simulation, backref=backref('timesteps', order_by=time_gyr,
                                                          cascade_backrefs=False),
                              cascade='')

    @property
    def escaped_extension(self):
        return self.extension.replace("/","%")

    @property
    def filename(self):
        return os.path.join(config.base, self.simulation.basename, self.extension)

    @property
    def relative_filename(self):
        return str(self.simulation.basename + "/" + self.extension)

    def load(self, mode=None):
        handler = self.simulation.get_output_handler()
        return handler.load_timestep(self.extension, mode=mode)

    def load_region(self, region_specification, mode=None):
        handler = self.simulation.get_output_handler()
        return handler.load_region(self.extension, region_specification, mode=mode)

    def __init__(self, simulation, extension):
        from . import creator
        self.extension = str(extension)
        self.simulation = simulation
        self.creator = creator.get_creator(Session.object_session(simulation))

    def __repr__(self):
        extra = ""
        if not self.available:
            extra += " unavailable"
        path = self.path
        if self.redshift is None:
            return "<TimeStep %r%s>"%(path,extra)
        else:
            return f"<TimeStep {path!r} z={self.redshift:.2f} t={self.time_gyr:.2f} Gyr{extra}>"

    def short(self):
        return "<TimeStep(... z=%.2f ...)>" % self.redshift

    def __getitem__(self, halo_identifier):
        from . import Session, SimulationObjectBase
        session = Session.object_session(self)
        if isinstance(halo_identifier, int):
            object_typecode, halo_number = 0, halo_identifier
        elif "." in halo_identifier:
            object_typecode, halo_number = list(map(int, halo_identifier.split(".")))
        elif "_" in halo_identifier:
            object_typecode, halo_number = halo_identifier.split("_")
            object_typecode = SimulationObjectBase.class_from_tag(object_typecode).__mapper_args__['polymorphic_identity']
            halo_number = int(halo_number)
        else:
            object_typecode, halo_number = 0, int(halo_identifier)
        return session.query(SimulationObjectBase).filter_by(timestep_id=self.id, halo_number=halo_number, object_typecode=object_typecode).first()

    @property
    def path(self):
        with Session.object_session(self).no_autoflush:
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
        Session.object_session(self)
        raise RuntimeError("Not implemented")

    def calculate_all(self, *plist, **kwargs):
        """Gather the specified properties from the child objects.

        The parameters passed name the properties (or live-calculations) to return.
        For example m,r = ts.calculate_all("mass","radius") generates an array of mass and
        radius for all objects in timestep ts.

        :param object_type: integer or string representing the particular object type
                            (e.g. 'halo', 'BH' or 'group'). If None (default), all
                            types are included.

        :param limit: maximum number of objects to use. If None (default), all are included.

        :param sanitize: if True (default), remove all rows where a result was not found for
                         any of the specified properties/live-calculations. Otherwise, return
                         None where no result could be obtained, guaranteeing the return of a row for
                         every object.

        :param order_by_halo_number: if True, order by halo number; otherwise by database ID (default)
        """

        from .. import live_calculation
        from . import Session
        from .halo import SimulationObjectBase

        object_typecode = None
        object_typetag = kwargs.get('object_type', kwargs.get('object_typetag',None))
        limit = kwargs.get('limit', None)
        sanitize = kwargs.get('sanitize', True)
        order_by_halo_number = kwargs.get('order_by_halo_number', False)

        if object_typetag:
            object_typecode = SimulationObjectBase.object_typecode_from_tag(object_typetag)

        if isinstance(plist[0], live_calculation.Calculation):
            property_description = plist[0]
        else:
            property_description = live_calculation.parser.parse_property_names(*plist)

        # must be performed in its own session as we intentionally load in a lot of
        # objects with incomplete lazy-loaded properties
        session = Session()
        try:
            halo_alias = SimulationObjectBase
            raw_query = session.query(SimulationObjectBase).filter_by(timestep_id=self.id)
            if order_by_halo_number:
                raw_query = raw_query.order_by(SimulationObjectBase.halo_number)
            if object_typecode is not None:
                raw_query = raw_query.filter_by(object_typecode=object_typecode)
            if limit:
                # old-style sqlalchemy: from_self required for onwards joins
                # raw_query = raw_query.limit(limit).from_self()
                halo_alias = aliased(SimulationObjectBase, raw_query.limit(limit).subquery())
                raw_query = session.query(halo_alias)

            query = property_description.supplement_halo_query(raw_query, halo_alias)
            sql_query_results = query.all()
            if sanitize:
                calculation_results = property_description.values_sanitized(sql_query_results,
                                                                            Session.object_session(self))
            else:
                calculation_results = property_description.values(sql_query_results, Session.object_session(self))
        finally:
            session.close()
        return calculation_results

    def gather_property(self, *args, **kwargs):
        """The old alias for calculate_all, retained for compatibility"""
        return self.calculate_all(*args, **kwargs)

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
               and_(direction_comparison, TimeStep.simulation == self.simulation,
                    # Strictly the below condition shouldn't be required. However,
                    # MySQL returns rows where Timestep.time_gyr == self.time_gyr when we ask for a
                    # strict inequality. Probably this is to do with a roundoff error in the round-trip
                    # to the database. We have to prevent this, otherwise one gets incorrect results.
                    TimeStep.id != self.id)
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
