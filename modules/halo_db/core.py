import weakref
import time
import datetime

import numpy as np
import sqlalchemy
import sqlalchemy.orm.session
from sqlalchemy import Index, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, LargeBinary, create_engine, orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker, clear_mappers, deferred
from sqlalchemy import and_
from sqlalchemy.orm.session import Session

from . import data_attribute_mapper
from .identifiers import get_halo_property_with_magic_strings
from . import config
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

    def extract(self, f):
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
            return f[ilist]
        else:
            return f[pt]

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
            f = pynbody.load(self.filename, only_header=True)
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

        if self.redshift is None:
            return "<TimeStep(" + str(self.simulation) + ",\"" + self.extension + "\") no time" + extra + ">"
        else:
            return "<TimeStep(" + str(self.simulation) + ",\"" + self.extension + "\") z=%.2f t=%.2f Gyr%s>" % (self.redshift, self.time_gyr, extra)

    def short(self):
        return "<TimeStep(... z=%.2f ...)>" % self.redshift

    def __getitem__(self, i):
        return self.halos[i]

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

    def gather_linked_property(self, other, *plist, **kwargs):
        """Gather up specified properties from the chlid halos,
        using arguments as in gather_property. However, additionally
        gather all these specified properties for the same halos in
        the timestep specified by "other", using stored DB links to establish
        a 1:1 correspondence. """

        # Step 1: get the other timestep

        if not isinstance(other, TimeStep):
            other = get_timestep(other)

        # Step 2: establish the linked list of halos
        linked_halos = []

        for l in self.links_from:
            if l.halo_to.timestep.id == other.id:
                linked_halos.append((l.halo_from, l.halo_to))

        for l in self.links_to:
            if l.halo_from.timestep.id == other.id:
                linked_halos.append((l.halo_to, l.halo_from))

        print "Found", len(linked_halos), "halos in common"

        # Step 3: get the data

        filt = kwargs.get("filt", lambda x: True)
        if filt is True:
            filt = default_filter

        out = [[] for i in xrange(len(plist))]

        for h1, h2 in linked_halos:
            if filt(h1) and filt(h2):
                try:
                    res = [(get_halo_property_with_magic_strings(h1, p), get_halo_property_with_magic_strings(h2, p)) for p in plist]

                    for a, p in zip(out, res):
                        a.append(p)

                except (KeyError, IndexError):
                    pass

        return [safe_asarray(x) for x in out]

    def gather_property(self, *plist, **kwargs):
        """Gather up the specified properties from the child
        halos. For each argument either name a property or, for array
        properties, you can use <propertyname>//<n> where n is the
        array element to return, or <propertyname>//+ or
        <propertyname>//- to return the maximum or minimum element.

        Pass a function filt to only return halos where
        filt(halo) is True. If filt=True, this function defaults
        to checking that Sub is 0"""

        filt = lambda x: True
        allow_none = False

        if kwargs.has_key("filt"):
            filt = kwargs["filt"]

        if kwargs.has_key("allow_none"):
            allow_none = kwargs["allow_none"]

        verbose = kwargs.get("verbose", False)

        if filt is True:
            filt = default_filter

        out = [[] for i in xrange(len(plist))]

        for h in self.halos.options(
                    sqlalchemy.orm.joinedload(Halo.all_properties)
                  ).all():
            try:
                if filt(h):
                    res = [get_halo_property_with_magic_strings(h, p) for p in plist]
                    if verbose:
                        print h, res
                    if (not any([r is None for r in res])) or allow_none:
                        for a, p in zip(out, res):
                            a.append(p)
                elif verbose:
                    print "reject - ", h['Sub']
            except (KeyError, IndexError):
                pass

        return [safe_asarray(x) for x in out]

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
        return "<Halo " + str(self.halo_number) + " of " + self.timestep.short() + " | NDM=%d NStar=%d NGas=%d >" % (self.NDM, self.NStar, self.NGas)

    def load(self, partial=False):
        h = construct_halo_cat(self.timestep, self.halo_type)
        if partial:
            return h.load_copy(self.halo_number)
        else:
            return h[self.halo_number]

    def __getitem__(self, key):
        # There are two possible strategies here. If some sort of joined load has been
        # executed, the properties are sitting waiting for us. If not, they are going
        # to be lazy loaded and we want to filter that lazy load.
        session = Session.object_session(self)
        key_id = get_dict_id(key, session=session)
        if 'all_properties' not in sqlalchemy.inspect(self).unloaded:
            # we've already got it from the DB, find it locally
            for x in self.all_properties:
                if x.name_id==key_id:
                    return x.data
            for x in self.links:
                if x.relation_id==key_id:
                    return x.halo_to
        else:
            # nothing has been loaded from the DB, so look for just this property


            try:
                return session.query(HaloProperty).filter_by(name_id=key_id, halo_id=self.id, deprecated=False).order_by(HaloProperty.id.desc()).first().data
            except AttributeError:
                pass
            try:
                return session.query(HaloLink).filter_by(relation_id=key_id, halo_from_id=self.id).first().halo_to
            except AttributeError:
                pass


        raise KeyError(key)

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

    def get_linked_halos_from_target(self, targ):
        session = Session.object_session(self)
        if isinstance(targ, str):
            targ = get_item(targ)
        if isinstance(targ, TimeStep):
            links_from = session.query(Halo).join("halo_to","timestep")\
                .filter(HaloLink.halo_from_id==self.id, TimeStep.id==targ.id).all()
            links_to = session.query(Halo).join("halo_from","timestep")\
                .filter(HaloLink.halo_to_id==self.id, TimeStep.id==targ.id).all()

            links = [x.halo_to for x in links_from]+[x.halo_from for x in links_to]
            return links
        elif isinstance(targ, Simulation):
            links_from = session.query(HaloLink).join("halo_to","timestep","simulation")\
                .filter(HaloLink.halo_from_id==self.id, Simulation.id==targ.id).all()

            links_to = session.query(HaloLink).join("halo_from","timestep","simulation")\
                .filter(HaloLink.halo_to_id==self.id, Simulation.id==targ.id).all()

            links = [x.halo_to for x in links_from]+[x.halo_from for x in links_to]

            return links
        else:
            raise ValueError, "Don't know how to find a link to this target"

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key, obj):
        key = get_or_create_dictionary_item(internal_session, key)
        if isinstance(obj, Halo):
            X = self.reverse_links.filter_by(halo_from_id=obj.id,relation_id=key.id).first()
            if X is None:
                X = internal_session.merge(HaloLink(self, obj, key))
            else:
                X.halo_to = obj
            X.creator = current_creator

        else:
            X = self.properties.filter_by(name_id=key.id).first()

            if X is not None:
                X.data = obj
            else:
                X = internal_session.merge(HaloProperty(self, key.id, obj))
            X.creator = current_creator

    def keys(self):
        names = []
        for p in self.properties:
            names.append(p.name.text)

        return names

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


    def _raw_list_property_cascade(self, *keys, **kwargs):
        on_missing = kwargs.get('on_missing','skip')
        output = []
        for key in keys:
            try:
                output.append([get_halo_property_with_magic_strings(self,key)])
            except Exception, e:
                if on_missing=="skip":
                    output = [[] for k in keys]
                    break
                elif on_missing=="none":
                    output.append([np.NaN])
                else:
                    raise

        next = self.next
        if next:
            remainder = next._raw_list_property_cascade(*keys, on_missing=on_missing)
            output = [o+r for o,r in zip(output,remainder)]

        return output


    def property_cascade(self, *keys, **kwargs):
        on_missing = kwargs.get('on_missing','skip')
        x = self._raw_list_property_cascade(*keys,on_missing=on_missing)
        if len(keys) == 1:
            try:
                return np.asarray(x)
            except:
                return x
        else:
            y = []
            for z in x:
                try:
                    y.append(np.asarray(z))
                except:
                    y.append(z)

            return y

    def reverse_property_cascade(self, *keys):

        if len(keys) == 1:
            key = keys[0]
            try:
                return [self[key]] + self.previous.reverse_property_cascade(key)
            except AttributeError:
                return [self[key]]
            except KeyError:
                return []
        else:
            rt = []
            try:
                x = self.previous.reverse_property_cascade(*keys)
                for key, casc in zip(keys, x):
                    rt.append([self[key]] + casc)
                return rt
            except AttributeError:
                return [[self[key]] for key in keys]
            except KeyError:
                return [[] for key in keys]

    @property
    def number_cascade(self):
        try:
            return [self.halo_number] + self.next.number_cascade
        except AttributeError:
            return [self.halo_number]

    @property
    def reverse_number_cascade(self):
        try:
            return [self.halo_number] + self.previous.reverse_number_cascade
        except AttributeError:
            return [self.halo_number]

    @property
    def next(self):
        try:
            return self._next
        except:
            pass

        session = Session.object_session(self)

        if self.halo_type == 1:
            next_ts = self.timestep.next
            if next_ts is None:
                self._next = None
            else:
                self._next = next_ts.halos.filter_by(
                    halo_type=1, halo_number=self.halo_number).first()
        else:

            # The following would allow for multiple time evolution targets (e.g. step-skipping) but for now I'll avoid doing this
            # (it's also slower)
            # next_ts = self.timestep.next
            # if next_ts==None :
            #    return None
            # allowed_targets = [q.id for q in next_ts.halos]
            # linkobj =  session.query(HaloLink).filter(and_(HaloLink.relationship=="time", HaloLink.halo_from_id==self.id, HaloLink.halo_to_id.in_(allowed_targets))).first()

            linkobj = session.query(HaloLink).filter(and_(HaloLink.relation_id == get_dict_id(
                "time",session=session), HaloLink.halo_from_id == self.id)).first()

            if linkobj is not None:
                self._next = linkobj.halo_to
            else:
                self._next = None

        return self._next

    @property
    def previous(self):
        try:
            return self._previous
        except:
            pass

        session = Session.object_session(self)

        if self.halo_type == 1:
            prev_ts = self.timestep.previous
            if prev_ts is None:
                self._previous = None
            else:
                self._previous = prev_ts.halos.filter_by(
                    halo_type=1, halo_number=self.halo_number).first()
        else:
            linkobj = session.query(HaloLink).filter(and_(
                HaloLink.relation_id == get_dict_id("time", session=session), HaloLink.halo_to_id == self.id)).first()

            if linkobj is not None:
                self._previous = linkobj.halo_from
            else:
                self._previous = None

        return self._previous

    def short(self):
        return "<Halo " + str(self.halo_number) + " of ...>"


class HaloProperty(Base):
    __tablename__ = 'haloproperties'

    id = Column(Integer, primary_key=True)
    halo_id = Column(Integer, ForeignKey('halos.id'))
    # n.b. backref defined below
    halo = relationship(Halo, cascade='none')

    data_float = Column(Float)
    data_array = deferred(Column(LargeBinary))
    data_int = Column(Integer)

    name_id = Column(Integer, ForeignKey('dictionary.id'))
    name = relationship(DictionaryItem)

    deprecated = Column(Boolean, default=False, nullable=False)

    creator = relationship(Creator, backref=backref(
        'properties', cascade='all', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    def __init__(self, halo, name, data):
        if type(halo) is Halo:
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
    def data(self):
        return data_attribute_mapper.get_data_of_unknown_type(self)

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

Halo.all_properties = relationship(HaloProperty, primaryjoin=(HaloProperty.halo_id == Halo.id) & (
                                  HaloProperty.deprecated == False))


class HaloLink(Base):
    __tablename__ = 'halolink'

    id = Column(Integer, primary_key=True)
    halo_from_id = Column(Integer, ForeignKey('halos.id'))
    halo_to_id = Column(Integer, ForeignKey('halos.id'))

    halo_from = relationship(Halo, primaryjoin=halo_from_id == Halo.id,
                             backref=backref('links', cascade='all, delete-orphan',
                                             lazy='dynamic',
                                             primaryjoin=halo_from_id == Halo.id),
                             cascade='')

    halo_to = relationship(Halo, primaryjoin=(halo_to_id == Halo.id),
                           backref=backref('reverse_links', cascade='all, delete-orphan',
                                           lazy='dynamic',
                                           primaryjoin=halo_to_id == Halo.id),
                           cascade='')



    creator_id = Column(Integer, ForeignKey('creators.id'))
    creator = relationship(Creator, backref=backref(
        'halolinks', cascade='all, delete', lazy='dynamic'), cascade='save-update')

    relation_id = Column(Integer, ForeignKey('dictionary.id'))
    relation = relationship(DictionaryItem, primaryjoin=(
        relation_id == DictionaryItem.id), cascade='save-update,merge')

    def __init__(self,  halo_from, halo_to, relationship):

        self.halo_from = halo_from
        self.halo_to = halo_to

        self.relation = relationship

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


def all_simulations(session=None):
    global internal_session
    if session is None:
        session = internal_session
    return session.query(Simulation).all()


def all_creators():
    return internal_session.query(Creator).all()



def cache_dict():
    global _dict_id

    for x in internal_session.query(DictionaryItem).all():
        _dict_id[x.text] = x.id


def get_dict_id(text, default=None, session=None):
    """Get a DictionaryItem id for text (possibly cached). Raises KeyError if
    no dictionary object exists for the specified text"""
    global _dict_id

    if session is None:
        session = internal_session

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


def construct_halo_cat(timestep_db, type_id):
    f = timestep_db.load()
    if type_id == 0 or type_id is None:
        # amiga grp halo
        h = _loaded_halocats.get(f, lambda: None)()
        if h is None:
            h = f.halos()
            _loaded_halocats[f] = weakref.ref(h)
        return h  # pynbody.halo.AmigaGrpCatalogue(f)
    elif type_id == 1:
        # tracker halo... load up tracking data
        trackers = timestep_db.simulation.trackers.all()
        d = {}
        for x in trackers:
            d[x.halo_number] = x.extract(f)
        return d


def get_timestep(id):
    if isinstance(id, str):
        sim, ts = id.split("/")
        sim = get_simulation(sim)
        res = internal_session.query(TimeStep).filter(
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
        return internal_session.query(TimeStep).filter_by(id=id).first()


def get_halo(id):
    if isinstance(id, str):
        sim, ts, halo = id.split("/")
        ts = get_timestep(sim + "/" + ts)
        if "." in halo:
            halo_type, halo_number = map(int, halo.split("."))
        else:
            halo_type, halo_number = 0, int(halo)
        return internal_session.query(Halo).filter_by(timestep_id=ts.id, halo_number=halo_number, halo_type=halo_type).first()
    return internal_session.query(Halo).filter_by(id=id).first()


def get_item(path):
    c = path.count("/")
    if c is 0:
        return get_simulation(path)
    elif c is 1:
        return get_timestep(path)
    elif c is 2:
        return get_halo(path)


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


class BlockingSession(Session):
    # def __init__(self,*params) :
    #    super(BlockingSession,self).__init__(*params)

    def execute(self, *params, **kwparams):
        retries = 20
        while retries > 0:
            try:
                return super(BlockingSession, self).execute(*params, **kwparams)
            except sqlalchemy.exc.OperationalError:
                super(BlockingSession,self).rollback()
                retries -= 1
                if retries > 0:
                    print "DB is locked (%d attempts remain)..." % retries
                    time.sleep(1)
                else:
                    raise
            except sqlalchemy.exc.IntegrityError, ex:
                import pdb
                pdb.set_trace()


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

def init_db():
    global _verbose, current_creator, internal_session, engine
    engine = create_engine('sqlite:///' + config.db, echo=_verbose,
                           isolation_level='READ UNCOMMITTED',  connect_args={'timeout': 10})
    current_creator = Creator()
    Session = sessionmaker(bind=engine)
    internal_session=Session()
    Base.metadata.create_all(engine)

def use_blocking_session():
    global internal_session, engine
    internal_session.commit()
    internal_session = BlockingSession(bind=engine)

init_db()

__all__ = ['DictionaryItem','Creator','Simulation','SimulationProperty','TrackData',
           'update_tracker_halos','safe_asarray','default_filter','use_blocking_session',
           'TimeStep','Halo','HaloProperty','HaloLink','ArrayPlotOptions',
           'all_simulations','all_creators','cache_dict','get_dict_id',
           'sim_query_from_name_list','sim_query_from_args','get_or_create_dictionary_item',
           'get_simulation','construct_halo_cat','get_timestep','get_halo','get_item',
           'get_haloproperty','copy_property','getdb','BlockingSession','supplement_argparser',
           'process_options','init_db','Base']
