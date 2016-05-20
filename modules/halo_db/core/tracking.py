import numpy as np
from sqlalchemy import Column, Integer, LargeBinary, Boolean, ForeignKey
from sqlalchemy.orm import relationship, backref

from . import Base
from . import creator
from .simulation import Simulation


class TrackData(Base):
    __tablename__ = 'trackdata'

    id = Column(Integer, primary_key=True)
    particle_array = Column(LargeBinary)

    use_iord = Column(Boolean, default=True, nullable=False)
    halo_number = Column(Integer, nullable=False, default=0)
    creator = relationship(creator.Creator, backref=backref(
        'trackdata', cascade='delete', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    simulation = relationship(Simulation, backref=backref('trackers', cascade='all, delete-orphan, merge',
                                                          lazy='dynamic', order_by=halo_number), cascade='save-update')

    def __init__(self, sim, halo_num=None):
        self.simulation = sim
        self.creator = creator.get_creator_id()
        if halo_num is None:
            hs = []
            for h in sim.trackers.all():
                hs.append(h.halo_number)
            if len(hs) > 0:
                halo_num = max(hs) + 1
            else:
                halo_num = 1
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
        self.particle_array = str(np.asarray(to, dtype=int).data)

    def get_indices_for_snapshot(self, f):
        pt = self.particles
        if self.use_iord is True:

            dm_part = f.dm[np.in1d(f.dm['iord'], pt)]

            try:
                star_part = f.star[np.in1d(f.star['iord'], pt)]
            except KeyError:
                star_part = f[0:0]

            try:
                gas_part = f.gas[np.in1d(f.gas['iord'], pt)]
            except KeyError:
                gas_part = f[0:0]

            #fx = dm_part.union(star_part)
            #fx = fx.union(gas_part)
            # return fx
            ilist = np.hstack((dm_part.get_index_list(f),
                               star_part.get_index_list(f),
                               gas_part.get_index_list(f)))
            ilist = np.sort(ilist)
            return ilist
        else:
            return pt

    def extract(self, f):
        return f[self.get_indices_for_snapshot(f)]

    def extract_as_copy(self, f):
        import pynbody
        indices = self.get_indices_for_snapshot(f)
        return pynbody.load(f.filename, take=indices)

    def select(self, f, use_iord=True):
        self.use_iord = use_iord
        if use_iord:
            pt = f['iord']
        else:
            pt = f.get_index_list(f.ancestor)
        self.particles = pt

    def __repr__(self):
        return "<TrackData %d of %s, len=%d" % (self.halo_number, repr(self.simulation), len(self.particles))

    def create_halos(self, first_ts=None, verbose=False):
        from . import Session
        from halo_db.core import Halo
        session = Session.object_session(self)
        if first_ts is None:
            create = True
        else:
            create = False
        for ts in self.simulation.timesteps:
            if ts == first_ts:
                create = True
            if not create:
                if verbose:
                    print ts, "-> precursor, don't create"
            elif ts.halos.filter_by(halo_number=self.halo_number, halo_type=1).count() == 0:
                h = Halo(ts, self.halo_number, 0, 0, 0, 1)
                session.add(h)
                if verbose:
                    print ts, "-> add"
            elif verbose:
                print ts, "exists"
        session.commit()

class TrackerHaloCatalogue(object):
    def __init__(self, f, trackers):
        self._sim = weakref.ref(f)
        self._trackers = trackers

    def __getitem__(self, item):
        tracker = self._trackers.filter_by(halo_number=item).first()
        return tracker.extract(self._sim())

    def load_copy(self, item):
        tracker = self._trackers.filter_by(halo_number=item).first()
        return tracker.extract_as_copy(self._sim())


def update_tracker_halos(sim=None):
    from . import internal_session
    from halo_db import get_simulation
    from halo_db.tools.terminalcontroller import heading

    if sim is None:
        x = internal_session.query(TrackData).all()
    else:
        x = get_simulation(sim).trackers.all()

    for y in x:
        heading(repr(y))
        y.create_halos()