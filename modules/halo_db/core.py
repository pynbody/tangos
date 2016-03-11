import weakref
import datetime

import numpy as np
import sqlalchemy
import sqlalchemy.orm.session
from sqlalchemy import Index, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, LargeBinary, create_engine, orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker, clear_mappers, deferred
from sqlalchemy import and_
from sqlalchemy.orm.session import Session

import data_attribute_mapper
import config
import time
import properties

_loaded_timesteps = {}
_loaded_halocats = {}
_dict_id = {}
_dict_obj = {}
_verbose = False
current_creator=None
internal_session=None
engine=None



clear_mappers()  # remove existing maps

Base = declarative_base()


class DictionaryItem(Base):
    __tablename__ = 'dictionary'

    id = Column(Integer, primary_key=True)
    text = Column(String, unique=True)

    def __repr__(self):
        return "<DictionaryItem " + self.text + ">"

    def __init__(self, text):
        self.text = text

    def providing_class(self):
        return properties.providing_class(self.text)

    def _x_fn(self, fn_name):
        cl = self.providing_class()
        response = getattr(cl, fn_name)()
        if isinstance(response, tuple):
            return response[cl().name().index(self.text)]
        else:
            return response

    def plot_x0(self):
        return self._x_fn("plot_x0")

    def plot_x_extent(self):
        return self._x_fn("plot_x_extent")

    def plot_x0(self):
        return self._x_fn("plot_x0")

    def plot_xdelta(self):
        return self._x_fn("plot_xdelta")

    def plot_xlabel(self):
        return self._x_fn("plot_xlabel")

    def plot_ylabel(self):
        return self._x_fn("plot_ylabel")

    def plot_yrange(self):
        return self._x_fn("plot_yrange")

    def plot_xlog(self):
        return self._x_fn("plot_xlog")

    def plot_ylog(self):
        return self._x_fn("plot_ylog")

    def plot_clabel(self):
        return self._x_fn("plot_clabel")



class Creator(Base):
    __tablename__ = 'creators'

    id = Column(Integer, primary_key=True)
    command_line = Column(String)
    dtime = Column(DateTime)
    host = Column(String)
    username = Column(String)
    cwd = Column(String)

    def __repr__(self):
        return "<Creator " + self.username + " on " + self.host + " @ " + self.dtime.strftime("%d/%m/%y %H:%M") + " via " + self.command_line.split(" ")[0].split("/")[-1] + ">"

    def __init__(self, argv=None):
        import socket
        import getpass
        import datetime
        import os
        if argv == None:
            import sys
            argv = sys.argv

        self.command_line = " ".join(argv)
        self.host = socket.gethostname()
        self.username = getpass.getuser()
        self.dtime = datetime.datetime.now()
        self.cwd = os.getcwd()

    def print_info(self):
        run = self
        print "*" * 60
        print "Run ID = ", self.id
        print "Command line = ", self.command_line
        print "Host = ", self.host
        print "Username = ", self.username
        print "Time = ", self.dtime.strftime("%d/%m/%y %H:%M")
        if len(run.simulations) > 0:
            print ">>>   ", len(run.simulations), "simulations"
        if run.timesteps.count() > 0:
            print ">>>   ", (run.timesteps).count(), "timesteps"
        if run.halos.count() > 0:
            print ">>>   ", run.halos.count(), "halos"
        if run.halolinks.count() > 0:
            print ">>>   ", run.halolinks.count(), "halolinks"
        if run.properties.count() > 0:
            print ">>>   ", run.properties.count(), "halo properties"
        if run.simproperties.count() > 0:
            print ">>>", run.simproperties.count(), "simulation properties"


class Simulation(Base):
    __tablename__ = 'simulations'
    # __table_args__ = {'useexisting': True}

    id = Column(Integer, primary_key=True)
    basename = Column(String)
    creator = relationship(
        Creator, backref=backref('simulations', cascade='all'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    def __init__(self, basename):
        self.basename = basename
        self.creator = current_creator

    def __repr__(self):
        return "<Simulation(\"" + self.basename + "\")>"

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, i):
        if isinstance(i, int):
            return self.timesteps[i]
        else:
            session = Session.object_session(self)
            did = get_dict_id(i, session=session)
            try:
                return session.query(SimulationProperty).filter_by(name_id=did,
                                                                   simulation_id=self.id).first().data
            except AttributeError:
                pass

        raise KeyError, i

    def __setitem__(self, st, data):
        assert isinstance(st, str)
        session = Session.object_session(self)
        name = get_or_create_dictionary_item(session, st)
        propobj = self.properties.filter_by(name_id=name.id).first()
        if propobj is None:
            propobj = session.merge(SimulationProperty(self, name, data))

        propobj.data = data
        session.commit()

    @property
    def path(self):
        return self.basename


class SimulationProperty(Base):
    __tablename__ = 'simulationproperties'

    id = Column(Integer, primary_key=True)
    name_id = Column(Integer, ForeignKey('dictionary.id'))
    name = relationship(DictionaryItem)

    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    simulation = relationship(Simulation, backref=backref('properties', cascade='all, delete-orphan',
                                                          lazy='dynamic', order_by=name_id), cascade='save-update')

    creator_id = Column(Integer, ForeignKey('creators.id'))
    creator = relationship(Creator, backref=backref(
        'simproperties', cascade='all, delete', lazy='dynamic'), cascade='save-update')

    data_float = Column(Float)
    data_int = Column(Integer)
    data_time = Column(DateTime)
    data_string = Column(String)
    data_array = Column(LargeBinary)


    def __init__(self, sim, name, data):
        self.simulation = sim
        self.name = name
        self.data = data
        self.creator = current_creator

    def data_repr(self):
        f = self.data
        if type(f) is float:
            x = "%.2g" % f
        elif type(f) is datetime.datetime:
            x = f.strftime('%H:%M %d/%m/%y')
        elif type(f) is str or type(f) is unicode:
            x = "'%s'" % f
        elif f is None:
            x = "None"
        elif isinstance(f, np.ndarray):
            x = str(f)
        else:
            x = "%d" % f

        return x

    def __repr__(self):
        x = "<SimulationProperty " + self.name.text + \
            " of " + self.simulation.__repr__()
        x += " = " + self.data_repr()
        x += ">"
        return x

    @property
    def data(self):
        return data_attribute_mapper.get_data_of_unknown_type(self)

    @data.setter
    def data(self, data):
        data_attribute_mapper.set_data_of_unknown_type(self, data)


class TrackData(Base):
    __tablename__ = 'trackdata'

    id = Column(Integer, primary_key=True)
    particle_array = Column(LargeBinary)

    use_iord = Column(Boolean, default=True, nullable=False)
    halo_number = Column(Integer, nullable=False, default=0)
    creator = relationship(Creator, backref=backref(
        'trackdata', cascade='delete', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    simulation = relationship(Simulation, backref=backref('trackers', cascade='all, delete-orphan, merge',
                                                          lazy='dynamic', order_by=halo_number), cascade='save-update')

    def __init__(self, sim, halo_num=None):
        self.simulation = sim
        self.creator = current_creator
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
                internal_session.add(h)
                if verbose:
                    print ts, "-> add"
            elif verbose:
                print ts, "exists"
        internal_session.commit()


def update_tracker_halos(sim=None):
    from terminalcontroller import heading

    if sim is None:
        x = internal_session.query(TrackData).all()
    else:
        x = get_simulation(sim).trackers.all()

    for y in x:
        heading(repr(y))
        y.create_halos()


def safe_asarray(x):
    try:
        return np.asarray(x)
    except:
        return x


def default_filter(halo):
    return halo.get("Sub") is None and halo.NDM > 15000


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
        return str(config.base + self.simulation.basename + "/" + self.extension)

    @property
    def relative_filename(self):
        return str(self.simulation.basename + "/" + self.extension)

    def load(self):
        f = _loaded_timesteps.get(self.relative_filename, lambda: None)()
        if f is None:
            import pynbody
            f = pynbody.load(self.filename)
            if isinstance(f, pynbody.snapshot.gadget.GadgetSnap):
                f.wrap()
            _loaded_timesteps[self.relative_filename] = weakref.ref(f)
        return f

    def __init__(self, simulation, extension, autoload=True):
        import pynbody
        import pynbody.analysis.cosmology
        ext = str(extension)
        while ext[0] == "/":
            ext = ext[1:]
        self.extension = str(extension)
        self.simulation = simulation
        if autoload:
            #f = pynbody.load(self.filename, only_header=True)
            f = pynbody.load(self.filename)
            self.redshift = f.properties['z']
            try:
                self.time_gyr = f.properties['time'].in_units("Gyr")
            except:
                self.time_gyr = -1
            self.available = True
        else:
            self.available = False

        self.creator = current_creator

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
        session = Session.object_session(self)
        raise RuntimeError, "Not implemented"

    def gather_property(self, *plist, **kwargs):
        """Gather up the specified properties from the child
        halos."""

        from . import live_calculation

        if isinstance(plist[0], live_calculation.Calculation):
            property_description = plist[0]
        else:
            property_description = live_calculation.parse_property_names(*plist)

        # must be performed in its own session as we intentionally load in a lot of
        # objects with incomplete lazy-loaded properties
        session = Session()
        raw_query = session.query(Halo).filter_by(timestep_id=self.id)

        query = property_description.supplement_halo_query(raw_query)
        results = query.all()
        return property_description.values_sanitized(results)


    @property
    def next(self):
        try:
            return self._next
        except:
            session = Session.object_session(self)
            self._next = session.query(TimeStep).filter(and_(
                TimeStep.time_gyr > self.time_gyr, TimeStep.simulation == self.simulation)).order_by(TimeStep.time_gyr).first()
            return self._next

    @property
    def previous(self):
        try:
            return self._previous
        except:
            session = Session.object_session(self)
            self._previous = session.query(TimeStep).filter(and_(
                TimeStep.time_gyr < self.time_gyr, TimeStep.simulation == self.simulation)).order_by(TimeStep.time_gyr.desc()).first()
            return self._previous


class Halo(Base):
    __tablename__ = 'halos'


    id = Column(Integer, primary_key=True)
    halo_number = Column(Integer)
    timestep_id = Column(Integer, ForeignKey('timesteps.id'))
    timestep = relationship(TimeStep, backref=backref(
        'halos', order_by=halo_number, cascade='all', lazy='dynamic'), cascade='save-update, merge')
    NDM = Column(Integer)
    NStar = Column(Integer)
    NGas = Column(Integer)
    creator = relationship(Creator, backref=backref(
        'halos', cascade='all', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))
    halo_type = Column(Integer, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity':0,
        'polymorphic_on':halo_type
    }

    def __init__(self, timestep, halo_number, NDM, NStar, NGas, halo_type=0):
        self.timestep = timestep
        self.halo_number = halo_number
        self.NDM = NDM
        self.NStar = NStar
        self.NGas = NGas
        self.halo_type = halo_type
        self.init_on_load()
        self.creator = current_creator

    @orm.reconstructor
    def init_on_load(self):
        self._dict_is_complete = False
        self._d = {}

    def __repr__(self):

        return "<%s %r | NDM=%d Nstar=%d Ngas=%d>"%(self.__class__.__name__, self.path, self.NDM, self. NStar, self.NGas)


    @property
    def path(self):
        if self.halo_type==0:
            name = str(self.halo_number)
        else:
            name = str(self.halo_type)+"."+str(self.halo_number)
        return self.timestep.path+"/"+name


    def load(self, partial=False):
        h = construct_halo_cat(self.timestep, self.halo_type)
        if partial:
            return h.load_copy(self.halo_number)
        else:
            return h[self.halo_number]

    def calculate(self, name):
        from . import live_calculation
        calculation = live_calculation.parse_property_name(name)
        value = calculation.values_sanitized([self])[0]
        if len(value)==1:
            return value[0]
        else:
            return value

    def get_x_values_for(self, key):
        obj = self._get_object(key)
        if len(obj)>1:
            raise ValueError, "More than one piece of data with the same key; cannot generate unambiguous x values"
        if not isinstance(obj[0], HaloProperty):
            raise TypeError, "No x-values for stored data of type %r"%type(obj[0])
        return obj[0].x_values()

    def __getitem__(self, key):
        # There are two possible strategies here. If some sort of joined load has been
        # executed, the properties are sitting waiting for us. If not, they are going
        # to be lazy loaded and we want to filter that lazy load.
        return self.get_data(key)

    def get_data(self, key, raw=False, always_return_array=False):
        return_objs = self._get_object(key)

        return_data = []

        for r in return_objs:
            if isinstance(r, HaloProperty):
                data = r.data_raw if raw else r.data
            elif isinstance(r, HaloLink):
                data = r.halo_to
            else:
                raise TypeError, "Unknown type of data encountered during internal get_item processing"
            return_data.append(data)

        if (not always_return_array) and len(return_data) == 1:
            return_data = return_data[0]

        return return_data

    def _use_fixed_cache(self):
        return 'all_properties' not in sqlalchemy.inspect(self).unloaded

    def _get_object(self, key):
        session = Session.object_session(self)
        key_id = get_dict_id(key, session=session)
        if self._use_fixed_cache():
            return_objs = self._get_object_cached(key_id)
        else:
            return_objs = self._get_object_from_session(key_id, session)
        if len(return_objs) == 0:
            raise KeyError, "No such property %r" % key
        return return_objs

    def _get_object_from_session(self, key_id, session):
        query_properties = session.query(HaloProperty).filter_by(name_id=key_id, halo_id=self.id,
                                                                 deprecated=False).order_by(HaloProperty.id.desc())

        ret_values = query_properties.all()
        query_links = session.query(HaloLink).filter_by(relation_id=key_id, halo_from_id=self.id)
        for link in query_links.all():
            ret_values.append(link)

        return ret_values

    def _get_object_cached(self, key_id):
        return_vals = []
        # we've already got it from the DB, find it locally
        for x in self.all_properties:
            if x.name_id == key_id:
                return_vals.append(x)

        for x in self.all_links:
            if x.relation_id == key_id:
                return_vals.append(x)

        return return_vals

    def get_property(self, key, default=None):
        session = Session.object_session(self)
        key_id = get_dict_id(key, session=session)
        prop= self.properties.filter_by(name_id=key_id,deprecated=False).first()
        if prop is None:
            return default
        else:
            return prop.data

    def get_linked_halo(self, key, default=None):
        session = Session.object_session(self)
        key_id = get_dict_id(key, session=session)
        prop= session.query(HaloLink).filter_by(halo_from_id=self.id,
                               relation_id=key_id).first()
        if prop is None:
            return default
        else:
            return prop.halo_to

    def get_reverse_linked_halo(self, key, default=None):
        session = Session.object_session(self)
        key_id = get_dict_id(key, session=session)
        prop= session.query(HaloLink).filter_by(halo_to_id=self.id,
                               relation_id=key_id).first()
        if prop is None:
            return default
        else:
            return prop.halo_from

    def get_either_linked_halo(self, key, default=None):
        ret = self.get_linked_halo(key)
        if ret is None:
            ret = self.get_reverse_linked_halo(key)
        if ret is None:
            ret = default
        return ret



    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key, obj):

        if isinstance(obj, Halo):
            self._setitem_one_halo(key, obj)
        elif hasattr(obj, '__len__') and all([isinstance(x,Halo) for x in obj]):
            self._setitem_multiple_halos(key, obj)
        else:
            self._setitem_property(key, obj)

    def _setitem_property(self, key, obj):
        session = Session.object_session(self)
        key = get_or_create_dictionary_item(internal_session, key)
        X = self.properties.filter_by(name_id=key.id).first()
        if X is not None:
            X.data = obj
        else:
            X = session.merge(HaloProperty(self, key, obj))
        X.creator = current_creator

    def _setitem_one_halo(self, key, obj):
        session = Session.object_session(self)
        key = get_or_create_dictionary_item(session, key)
        X = self.links.filter_by(halo_from_id=self.id, relation_id=key.id).first()
        if X is None:
            X = session.merge(HaloLink(self, obj, key))
            X.creator = current_creator
        else:
            X.halo_to = obj

    def _setitem_multiple_halos(self, key, obj):
        session = Session.object_session(self)
        key = get_or_create_dictionary_item(session, key)
        self.links.filter_by(halo_from_id=self.id, relation_id=key.id).delete()
        links = [HaloLink(self, halo_to, key) for halo_to in obj]
        session.add_all(links)


    def keys(self):
        names = []
        if self._use_fixed_cache():
            props = self.all_properties
            links = self.all_links
        else:
            props = self.properties
            links = self.links

        for p in props:
            names.append(p.name.text)
        for p in links:
            names.append(p.relation.text)

        return names

    def __contains__(self, item):
        return item in self.keys()

    @property
    def tracker(self):
        if self.halo_type != 1:
            return None
        else:
            return self.timestep.simulation.trackers.filter_by(halo_number=self.halo_number).first()

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

    def plot(self, name, *args, **kwargs):
        name_id = get_dict_id(name, Session.object_session(self))
        data = self.properties.filter_by(name_id=name_id).first()
        return data.plot(*args, **kwargs)


    def property_cascade(self, *plist, **kwargs):
        """Run the specified calculations on this halo and its descendants

        Each argument is a string (or an instance of live_calculation.Calculation), following the syntax
        described in live_calculation.md.

        *kwargs*:

        :param nmax: The maximum number of descendants to consider (default 1000)
        :param hop_class: The class to use to find the descendants (default halo_finder.MultiHopMajorDescendantsStrategy)
        """
        from . import live_calculation
        from . import halo_finder
        from . import temporary_halolist as thl

        nmax = kwargs.get('nmax',1000)
        hop_class = kwargs.get('hop_class', halo_finder.MultiHopMajorDescendantsStrategy)

        if isinstance(plist[0], live_calculation.Calculation):
            property_description = plist[0]
        else:
            property_description = live_calculation.parse_property_names(*plist)

        # must be performed in its own session as we intentionally load in a lot of
        # objects with incomplete lazy-loaded properties
        session = Session()
        with hop_class(get_halo(self.id, session), nhops_max=nmax, include_startpoint=True).temp_table() as tt:
            raw_query = thl.halo_query(tt)
            query = property_description.supplement_halo_query(raw_query)
            results = query.all()
            return property_description.values_sanitized(results)

    def reverse_property_cascade(self, *plist, **kwargs):
        """Run the specified calculations on the progenitors of this halo

        For more information see property_cascade.
        """
        from . import halo_finder
        kwargs['hop_class'] = halo_finder.MultiHopMajorProgenitorsStrategy
        return self.property_cascade(*plist, **kwargs)


    @property
    def next(self):
        if not hasattr(self, '_next'):
            from . import halo_finder
            strategy = halo_finder.HopMajorDescendantStrategy(self)
            self._next=strategy.first()

        return self._next

    @property
    def previous(self):
        if not hasattr(self, '_previous'):
            from . import halo_finder
            strategy = halo_finder.HopMajorProgenitorStrategy(self)
            self._previous=strategy.first()

        return self._previous

    def short(self):
        return "<Halo " + str(self.halo_number) + " of ...>"


class BH(Halo):
    __mapper_args__ = {
        'polymorphic_identity':1
    }

    def __init__(self, timestep, halo_number):
        super(BH, self).__init__(timestep, halo_number, 0,0,0,1)

    @property
    def host_halo(self):
        try:
            match =  self.reverse_links.filter_by(relation_id=get_dict_id('BH_central')).first()
        except KeyError:
            match =  self.reverse_links.filter_by(relation_id=get_dict_id('BH')).first()

        if match is None:
            return None
        else:
            return match.halo_from

class HaloProperty(Base):
    __tablename__ = 'haloproperties'

    id = Column(Integer, primary_key=True)
    halo_id = Column(Integer, ForeignKey('halos.id'))
    # n.b. backref defined below
    halo = relationship(Halo, cascade='none', backref=backref('all_properties'))

    data_float = Column(Float)
    data_array = Column(LargeBinary)
    data_int = Column(Integer)

    name_id = Column(Integer, ForeignKey('dictionary.id'))
    name = relationship(DictionaryItem)

    deprecated = Column(Boolean, default=False, nullable=False)

    creator = relationship(Creator, backref=backref(
        'properties', cascade='all', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    def __init__(self, halo, name, data):
        if isinstance(halo, Halo):
            self.halo = halo
        else:
            self.halo_id = halo

        self.name = name
        self.data = data
        self.creator = current_creator

    def __repr__(self):
        if self.deprecated:
            x = "<HaloProperty (deprecated)"
        else:
            x = "<HaloProperty"
        if self.data_float is not None:
            return (x + self.name.text + "=%.2e" % self.data) + " of " + self.halo.short() + ">"
        elif self.data_array is not None:
            return x + self.name.text + " (array) of " + self.halo.short() + ">"
        elif self.data_int is not None:
            return x + self.name.text + "=" + str(self.data_int) + " of " + self.halo.short() + ">"
        else:
            return x + ">"  # shouldn't be in this state

    def data_is_array(self):
        """Return True if data is an array (without loading the array)"""
        return (self.data_int is None) and (self.data_float is None)

    @property
    def data_raw(self):
        return data_attribute_mapper.get_data_of_unknown_type(self)

    @property
    def data(self):
        try:
            cls = self.name.providing_class()
        except NameError:
            cls = None

        if hasattr(cls, 'reassemble'):
            return cls.reassemble(self)
        else:
            return self.data_raw

    def x_values(self):
        if not self.data_is_array():
            raise ValueError, "The data is not an array"
        return self.name.providing_class().plot_x_values(self.data)

    def plot(self, *args, **kwargs):
        xdat = self.x_values()
        import matplotlib.pyplot as p
        return p.plot(xdat, self.data, *args, **kwargs)

    @data.setter
    def data(self, data):
        data_attribute_mapper.set_data_of_unknown_type(self, data)


Halo.properties = relationship(HaloProperty, cascade='all', lazy='dynamic',
                               primaryjoin=(HaloProperty.halo_id == Halo.id) & (
                                   HaloProperty.deprecated == False),
                               uselist=True)


Halo.deprecated_properties = relationship(HaloProperty, cascade='all',
                                          primaryjoin=(HaloProperty.halo_id == Halo.id) & (
                                              HaloProperty.deprecated == True),
                                          uselist=True)

# eager loading support:

#Halo.all_properties = relationship(HaloProperty, primaryjoin=(HaloProperty.halo_id == Halo.id) & (
#                                  HaloProperty.deprecated == False))


class HaloLink(Base):
    __tablename__ = 'halolink'

    id = Column(Integer, primary_key=True)
    halo_from_id = Column(Integer, ForeignKey('halos.id'))
    halo_to_id = Column(Integer, ForeignKey('halos.id'))

    halo_from = relationship(Halo, primaryjoin=halo_from_id == Halo.id,
                             backref=backref('links', cascade='all',
                                             lazy='dynamic',
                                             primaryjoin=halo_from_id == Halo.id),
                             cascade='')

    halo_to = relationship(Halo, primaryjoin=(halo_to_id == Halo.id),
                           backref=backref('reverse_links', cascade='all, delete-orphan',
                                           lazy='dynamic',
                                           primaryjoin=halo_to_id == Halo.id),
                           cascade='')

    weight = Column(Float)

    creator_id = Column(Integer, ForeignKey('creators.id'))
    creator = relationship(Creator, backref=backref(
        'halolinks', cascade='all, delete', lazy='dynamic'), cascade='save-update')

    relation_id = Column(Integer, ForeignKey('dictionary.id'))
    relation = relationship(DictionaryItem, primaryjoin=(
        relation_id == DictionaryItem.id), cascade='save-update,merge')



    def __init__(self,  halo_from, halo_to, relationship, weight=None):

        self.halo_from = halo_from
        self.halo_to = halo_to

        self.relation = relationship
        self.weight = weight

        self.creator = current_creator

    def __repr__(self):
        return "<HaloLink " + str(self.relation.text) + " " + str(self.halo_from) + " " + str(self.halo_to) + ">"

TimeStep.links_from = relationship(HaloLink, secondary=Halo.__table__,
                                   secondaryjoin=(
                                       HaloLink.halo_from_id == Halo.id),
                                   primaryjoin=(
                                       Halo.timestep_id == TimeStep.id),
                                   cascade='none', lazy='dynamic')



TimeStep.links_to = relationship(HaloLink, secondary=Halo.__table__,
                                 secondaryjoin=(
                                     HaloLink.halo_to_id == Halo.id),
                                 primaryjoin=(Halo.timestep_id == TimeStep.id),
                                 cascade='none', lazy='dynamic')



Halo.all_links = relationship(HaloLink, primaryjoin=(HaloLink.halo_from_id == Halo.id))



class ArrayPlotOptions(Base):
    __tablename__ = 'arrayplotoptions'

    id = Column(Integer, primary_key=True)
    dictionary_id = Column(Integer, ForeignKey('dictionary.id'))
    relates_to = relationship(DictionaryItem, backref=backref(
        'array_plot_options', cascade='all,delete-orphan'))

    labelx = Column(String)
    labely = Column(String)

    x0 = Column(Float, default=0)
    dx = Column(Float, default=1)
    dx_is_logarithmic = Column(Boolean, default=False)

    plot_x_logarithmic = Column(Boolean, default=False)
    plot_y_logarithmic = Column(Boolean, default=False)

    use_range = Column(Boolean, default=False)

    def __init__(self, dictionary_item=None):
        self.relates_to = dictionary_item


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



def _get_dict_cache_for_session(session):
    session_dict = _dict_id.get(session, {})
    _dict_id[session] = session_dict
    return session_dict

def cache_dict():

    session_dict = _get_dict_cache_for_session(internal_session)

    for x in internal_session.query(DictionaryItem).all():
        session_dict[x.text] = x.id


def get_dict_id(text, default=None, session=None):
    """Get a DictionaryItem id for text (possibly cached). Raises KeyError if
    no dictionary object exists for the specified text"""


    if session is None:
        session = internal_session

    _dict_id = _get_dict_cache_for_session(session)

    try:
        return _dict_id[text]
    except KeyError:

        try:
            obj = session.query(DictionaryItem).filter_by(text=text).first()
        except:
            if default is None:
                raise
            else:
                return default

        if obj is None:
            if default is None:
                raise
            else:
                return default

        _dict_id[text] = obj.id
        return obj.id

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



def get_or_create_dictionary_item(session, name):
    """This tries to get the DictionaryItem corresponding to name from
    the database.  If it doesn't exist, it creates a pending
    object. Note that this must be called *while the database is
    locked under the specified session* to prevent duplicate items
    being created"""

    if session not in _dict_obj:
        _dict_obj[session] = {}

    # try to get it from the cache
    obj = _dict_obj[session].get(name, None)

    if obj is not None:
        return obj

    # try to get it from the db
    obj = session.query(DictionaryItem).filter_by(text=name).first()


    if obj is None:
        # try to create it
        try:
            obj = DictionaryItem(name)
            obj = session.merge(obj)
            session.commit()
        except sqlalchemy.exc.IntegrityError:
            print " -> failed dictionary creation attempt"
            session.rollback()
            obj = session.query(DictionaryItem).filter_by(text=name).first()
            if obj is None:
                raise # can't get it from the DB, can't create it from the DB... who knows...

    _dict_obj[session][name] = obj
    return obj



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


def construct_halo_cat(timestep_db, type_id):
    f = timestep_db.load()
    if type_id == 0 or type_id is None:
        # amiga grp halo
        h = _loaded_halocats.get(id(f), lambda: None)()
        if h is None:
            h = f.halos()
            _loaded_halocats[id(f)] = weakref.ref(h)
        return h  # pynbody.halo.AmigaGrpCatalogue(f)
    elif type_id == 1:
        return TrackerHaloCatalogue(f,timestep_db.simulation.trackers)


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
                    if isinstance(item, Simulation) and cl is Halo :
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
    global _verbose, current_creator, internal_session, engine, Session
    if db_uri is None:
        db_uri = 'sqlite:///' + config.db
    engine = create_engine(db_uri, echo=verbose or _verbose,
                           isolation_level='READ UNCOMMITTED',  connect_args={'timeout': timeout})
    current_creator = Creator()
    Session = sessionmaker(bind=engine)
    internal_session=Session()
    Base.metadata.create_all(engine)

def use_blocking_session():
    global engine
    from . import blocking_session
    blocking_session.make_engine_blocking(engine)

init_db()

__all__ = ['DictionaryItem','Creator','Simulation','SimulationProperty','TrackData',
           'update_tracker_halos','safe_asarray','default_filter','use_blocking_session',
           'TimeStep','Halo','HaloProperty','HaloLink','ArrayPlotOptions',
           'all_simulations','all_creators','cache_dict','get_dict_id',
           'sim_query_from_name_list','sim_query_from_args','get_or_create_dictionary_item',
           'get_simulation','construct_halo_cat','get_timestep','get_halo','get_item',
           'get_haloproperty','copy_property','getdb', 'supplement_argparser',
           'process_options','init_db','Base']
