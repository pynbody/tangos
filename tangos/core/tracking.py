import numpy as np
from sqlalchemy import Boolean, Column, ForeignKey, Integer, func
from sqlalchemy.orm import Session, backref, relationship

from ..config import LARGE_BINARY
from ..log import logger
from . import Base, creator
from .dictionary import get_or_create_dictionary_item
from .halo import Tracker
from .halo_data import HaloLink
from .simulation import Simulation
from .timestep import TimeStep


class TrackData(Base):
    __tablename__ = 'trackdata'

    id = Column(Integer, primary_key=True)
    particle_array = Column(LARGE_BINARY)

    use_iord = Column(Boolean, default=True, nullable=False)
    halo_number = Column(Integer, nullable=False, default=0)
    creator = relationship(creator.Creator, backref=backref(
        'trackdata', cascade_backrefs=False, lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    simulation = relationship(Simulation, backref=backref('trackers', cascade_backrefs=False,
                                                          lazy='dynamic', order_by=halo_number), cascade='')

    def __init__(self, sim, halo_num=None):
        session = Session.object_session(sim)
        self.simulation = sim
        self.creator = creator.get_creator()
        if halo_num is None:
            with session.no_autoflush:
                hmax, = session.query(func.max(TrackData.halo_number)).filter_by(simulation_id=sim.id).first()
                if hmax is None:
                    halo_num = 1
                else:
                    halo_num = hmax+1

        self.halo_number = halo_num
        self.use_iord = True
        self.particles = []

    @property
    def particles(self):
        try:
            return np.frombuffer(self.particle_array, dtype=int)
        except ValueError:
            return np.empty(shape=0, dtype=int)

    @particles.setter
    def particles(self, to):
        self.particle_array = np.asarray(to, dtype=int).data



    def select(self, f, use_iord=True):
        self.use_iord = use_iord
        if use_iord:
            pt = f['iord']
        else:
            pt = f.get_index_list(f.ancestor)
        self.particles = pt

    def __repr__(self):
        return "<TrackData %d of %s, len=%d" % (self.halo_number, repr(self.simulation), len(self.particles))

    def create_objects(self, class_=Tracker, first_timestep=None):
        from . import Session
        session = Session.object_session(self)

        timesteps = self.simulation.timesteps
        timesteps_id = [x.id for x in timesteps]

        if first_timestep is not None:
            first_index = timesteps_id.index(first_timestep.id)
            timesteps = timesteps[first_index:]

        for ts in timesteps:
            existing_query = session.query(class_).filter_by(halo_number = self.halo_number, timestep_id = ts.id)

            if existing_query.count() == 0:
                h = class_(ts, self.halo_number)
                h.tracker_id = self.id
                session.add(h)
                logger.info("Added a %s to %r",class_.__name__,ts)
            else:
                logger.debug("This tracker is already present in %r",ts)

        session.commit()

    def create_links(self, class_=Tracker):
        from . import Session
        session = Session.object_session(self)
        all_ = session.query(class_).join(TimeStep, TimeStep.id==class_.timestep_id).\
                                          filter((class_.halo_number == self.halo_number) &
                                                 (TimeStep.simulation_id == self.simulation_id)).\
                                          order_by(TimeStep.time_gyr).all()

        connection_name = get_or_create_dictionary_item(session, 'tracker_connection')
        for h1,h2 in zip(all_[1:],all_[:-1]):
            l1 = HaloLink(h1,h2,connection_name)
            l2 = HaloLink(h2,h1,connection_name)
            session.add_all([l1,l2])
        session.commit()


def update_tracker_halos(sim=None):
    from tangos import get_simulation
    from tangos.core import get_default_session
    from tangos.util.terminalcontroller import heading

    if sim is None:
        x = get_default_session().query(TrackData).all()
    else:
        x = get_simulation(sim).trackers.all()

    for y in x:
        heading(repr(y))
        y.create_objects()
