import numpy as np
from sqlalchemy import BigInteger, Column, ForeignKey, Integer, orm, types
from sqlalchemy.orm import Session, backref, relationship

from . import Base, creator, extraction_patterns
from .dictionary import get_dict_id, get_or_create_dictionary_item
from .timestep import TimeStep


class UnsignedInteger(types.TypeDecorator):
    """Stores an unsigned int64 as a signed int64"""

    impl = types.Integer

    cache_ok = True

    def process_bind_param(self, value, dialect):
        return np.uint64(value).astype(np.int64)

    def process_result_value(self, value, dialect):
        if value is not None:
            if type(value) is bytes:
                return np.frombuffer(value, dtype=np.uint64)[0]
            else:
                return np.int64(value).astype(np.uint64)
        else:
            return None

class SimulationObjectBase(Base):
    __tablename__= "halos"

    id = Column(Integer, primary_key=True) # the unique ID value of the database object created for this halo
    halo_number = Column(BigInteger) # by default this will be the halo's rank in terms of particle count
    finder_id = Column(BigInteger) # raw halo ID from the halo catalog, now passed to pynbody
    finder_offset = Column(BigInteger) # DEPRECATED - used to be the halo number in pynbody pre-v2
    timestep_id = Column(Integer, ForeignKey('timesteps.id'))
    timestep = relationship(TimeStep, backref=backref(
        'objects', order_by=halo_number, cascade_backrefs=False, lazy='dynamic'), cascade='')
    NDM = Column(Integer)
    NStar = Column(Integer)
    NGas = Column(Integer)
    creator = relationship(creator.Creator, backref=backref(
        'halos', cascade_backrefs=False, lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))
    object_typecode = Column(Integer, nullable=False,
                         name='halo_type') # name for backwards compatibility

    tag = "abstract_base_class_for_halos_etc" # tag will be halo, bh, tracker etc. Don't use the base class!

    __mapper_args__ = {
        'polymorphic_identity':100,
        'polymorphic_on':object_typecode
    }

    @classmethod
    def _all_subclasses(cls):
        for c in cls.__subclasses__():
            yield c
            yield from c._all_subclasses()

    @staticmethod
    def class_from_tag(match_tag):
        match_tag = match_tag.lower()
        for c in SimulationObjectBase._all_subclasses():
            if match_tag == c.tag.lower():
                return c
        raise ValueError("Unknown object type %r"%match_tag)

    @staticmethod
    def object_typecode_from_tag(match_tag):
        if isinstance(match_tag, str):
            return SimulationObjectBase.class_from_tag(match_tag).__mapper_args__['polymorphic_identity']
        else:
            return match_tag

    @staticmethod
    def object_typetag_from_code(typecode):
        for c in SimulationObjectBase._all_subclasses():
            if c.__mapper_args__['polymorphic_identity'] == typecode:
                return c.tag
        raise ValueError("Unknown object typecode %d",typecode)

    @staticmethod
    def typecode_and_number_from_human_identifier(identifier):
        """Return object typecode and halo number given a human identification string or number

        E.g. identifier = 5 -> typecode=0,
        identifier = '1.5' -> typecode=1, number = 5
        identifier = 'bh_5' -> typecode=1, number = 5
        """
        if isinstance(identifier, int):
            object_typecode, object_number = 0, identifier
        elif "." in identifier:
            object_typecode, object_number = list(map(int, identifier.split(".")))
        elif "_" in identifier:
            object_typecode, object_number = identifier.split("_")
            object_typecode = SimulationObjectBase.class_from_tag(object_typecode).__mapper_args__['polymorphic_identity']
            object_number = int(object_number)
        else:
            object_typecode, object_number = 0, int(identifier)

        return object_typecode, object_number

    def __init__(self, timestep, halo_number, finder_id, finder_offset, NDM, NStar, NGas, object_typecode=None):
        self.timestep = timestep
        self.halo_number = int(halo_number)
        self.finder_id = int(finder_id)
        self.finder_offset = int(finder_offset)
        self.NDM = int(NDM)
        self.NStar = int(NStar)
        self.NGas = int(NGas)
        if object_typecode is not None:
            self.object_typecode = int(object_typecode)
        self.init_on_load()
        self.creator = creator.get_creator(Session.object_session(timestep))

    @orm.reconstructor
    def init_on_load(self):
        self._dict_is_complete = False
        self._d = {}

    def __repr__(self):

        return "<%s %r | NDM=%d Nstar=%d Ngas=%d>"%(self.__class__.__name__, self.path, self.NDM, self. NStar, self.NGas)


    @property
    def basename(self):
        return self.tag+"_"+str(self.halo_number)

    @property
    def path(self):
        return self.timestep.path+"/"+self.basename

    @property
    def handler(self):
        if not hasattr(self, "_handler"):
            self._handler = self.timestep.simulation.get_output_handler()
        return self._handler

    @property
    def handler_class(self):
        if not hasattr(self, "_handler_class"):
            self._handler_class = self.timestep.simulation.output_handler_class
        return self._handler_class

    def load(self, mode=None):
        """Load the data for this halo, if it is present on this computer's filesystem.

        :param mode: the load mode to pass to the relevant input handler. For example with the pynbody input
        handler this can be None or 'partial' (in a normal session) and, when running inside an MPI session,
        'server' or 'server-partial'. See https://pynbody.github.io/tangos/mpi.html.
        """
        halo_number = self.halo_number
        if not hasattr(self, "finder_id"):
            finder_id = self.halo_number # backward compatibility
        else:
            finder_id = self.finder_id
        if not hasattr(self, "finder_offset"):
            finder_offset = finder_id
        else:
            finder_offset = self.finder_offset

        return self.handler.load_object(self.timestep.extension, finder_id, finder_offset, object_typetag=self.tag, mode=mode)

    def calculate(self, calculation, return_description=False):
        """Use the live-calculation system to calculate a user-specified function of the stored data.

        See the data exploration tutorials at https://pynbody.github.io/tangos/data_exploration.html
        for an introduction to the system.

        :param calculation: the calculation (or a string representation of it to be parsed)
        :param return_description: if True, return both the value and the PropertyCalculation class describing it.
        :returns: The result of the calculation, or a tuple containing the result and the description if
                  return_description is True.

        """

        from .. import live_calculation
        calculation = live_calculation.parser.parse_property_name_if_required(calculation)
        (value,), description = calculation.values_sanitized_and_description([self], Session.object_session(self))
        if len(value)==1:
            retval = value[0]
        else:
            retval = value

        if return_description:
            return retval, description
        else:
            return retval

    def __getitem__(self, key):
        """Highest-level method for retrieving data or link"""
        return self.get_data(key)

    def get(self, key, default=None):
        """Highest-level method for retrieving data or link, with default return value if no data is found
        under the specified key"""
        try:
            return self.get_data(key)
        except KeyError:
            return default

    def get_data(self, key, raw=False, always_return_array=False):
        """High-level data access to the property or linked halo named by key.

        :param key: string with the name of the property or link
        :param raw: if True, get the raw data rather than attempt to reassemble the data into a science-ready form
        :param always_return_array: if True, always return the data as a list of values, even if there is only one
           stored property with the specified name
        """

        if raw:
            getters=[extraction_patterns.HaloPropertyRawValueGetter()]
        else:
            getters=[extraction_patterns.HaloPropertyValueGetter()]
        getters+=[extraction_patterns.HaloLinkTargetGetter()]

        return_data = self.get_objects(key, getters)

        if (not always_return_array) and len(return_data) == 1:
            return_data = return_data[0]

        return return_data

    def get_objects(self, key, getters=None):
        """Get objects belonging to this halo named by the specified key.

        Compared to get_data, this allows access to the underlying HaloProperty or HaloLink objects, or to perform
        custom processing of the data by specifying particular extraction patterns to getters. For more information,
        see halo_data_extraction_patterns."""
        if getters is None:
            getters = [extraction_patterns.HaloPropertyGetter(), extraction_patterns.HaloLinkGetter()]
        from . import Session
        session = Session.object_session(self)
        key_id = get_dict_id(key, session=session)

        ret_values = []
        for g in getters:
            ret_values += g.get(self, key_id, session)

        if len(ret_values) == 0:
            raise KeyError("No such property %r" % key)
        return ret_values

    def get_description(self, key, getters=None):
        """Get a description of a named property or link, in the form of an object capable of calculating it.

        This can be helpful to extract meta-data such as the size of an image or steps of an array."""
        if getters is None:
            getters = [extraction_patterns.HaloPropertyGetter(), extraction_patterns.HaloLinkGetter()]
        object = self.get_objects(key, getters)[0]
        return object.description


    def __setitem__(self, key, obj):
        if isinstance(obj, SimulationObjectBase):
            self._setitem_one_halo(key, obj)
        elif hasattr(obj, '__len__') and all([isinstance(x,SimulationObjectBase) for x in obj]):
            self._setitem_multiple_halos(key, obj)
        else:
            self._setitem_property(key, obj)

    def _setitem_property(self, key, obj):
        from . import Session
        from .halo_data import HaloProperty

        session = Session.object_session(self)
        key = get_or_create_dictionary_item(session, key)
        X = self.properties.filter_by(name_id=key.id).first()
        if X is not None:
            X.data = obj
            X.creator = creator.get_creator(session)
        else:
            X = HaloProperty(self, key, obj)
            X.creator = creator.get_creator(session)
            session.add(X)
        session.commit()

    def _setitem_one_halo(self, key, obj):
        from . import HaloLink, Session
        session = Session.object_session(self)
        key = get_or_create_dictionary_item(session, key)
        X = self.links.filter_by(halo_from_id=self.id, relation_id=key.id).first()
        if X is None:
            X = HaloLink(self, obj, key)
            X.creator = creator.get_creator(session)
            session.add(X)
        else:
            X.halo_to = obj
            X.creator = creator.get_creator(session)
        session.commit()

    def _setitem_multiple_halos(self, key, obj):
        from . import Session
        from .halo_data import HaloLink

        session = Session.object_session(self)
        key = get_or_create_dictionary_item(session, key)
        self.links.filter_by(halo_from_id=self.id, relation_id=key.id).delete()
        links = [HaloLink(self, halo_to, key) for halo_to in obj]
        session.add_all(links)
        session.commit()


    def keys(self, getters = None):
        if getters is None:
            getters = [extraction_patterns.HaloPropertyGetter(), extraction_patterns.HaloLinkGetter()]
        from . import Session
        names = []
        session = Session.object_session(self)
        for g in getters:
            names+=g.keys(self, session)

        return names

    def __contains__(self, item):
        return item in list(self.keys())

    @property
    def earliest(self):
        if not hasattr(self, '_earliest'):
            from .. import relation_finding
            strategy = relation_finding.MultiHopMajorProgenitorsStrategy(self,order_by=['time_asc'],include_startpoint=True)
            self._earliest=strategy.first()

        return self._earliest

    @property
    def latest(self):
        if not hasattr(self, '_latest'):
            from .. import relation_finding
            strategy = relation_finding.MultiHopMajorDescendantsStrategy(self,order_by=['time_desc'],include_startpoint=True)
            self._latest=strategy.first()

        return self._latest

    def plot(self, name, *args, **kwargs):
        from . import Session
        name_id = get_dict_id(name, Session.object_session(self))
        data = self.properties.filter_by(name_id=name_id).first()
        return data.plot(*args, **kwargs)


    def calculate_for_descendants(self, *plist, **kwargs):
        """Run the specified calculations on this halo and its descendants

        Each argument is a string (or an instance of live_calculation.Calculation), following the syntax
        described in live_calculation.md.

        *kwargs*:

        :param nmax: The maximum number of descendants to consider (default 1000)
        :param strategy: The class to use to find the descendants (default relation_finding.MultiHopMajorDescendantsStrategy)
        """
        from .. import (
            live_calculation,
            query as db_query,
            relation_finding,
            temporary_halolist as thl,
        )
        from . import Session

        nmax = kwargs.get('nmax',1000)
        strategy = kwargs.get('strategy', relation_finding.MultiHopMajorDescendantsStrategy)
        strategy_kwargs = kwargs.get('strategy_kwargs', {})

        if isinstance(plist[0], live_calculation.Calculation):
            property_description = plist[0]
        else:
            property_description = live_calculation.parser.parse_property_names(*plist)

        # must be performed in its own session as we intentionally load in a lot of
        # objects with incomplete lazy-loaded properties
        session = Session()
        try:
            with strategy(db_query.get_halo(self.id, session), nhops_max=nmax,
                          include_startpoint=True, **strategy_kwargs).temp_table() as tt:
                raw_query = thl.halo_query(tt)
                query = property_description.supplement_halo_query(raw_query)
                results = query.all()
                return property_description.values_sanitized(results, Session.object_session(self))
        finally:
            session.close()

    def calculate_for_progenitors(self, *plist, **kwargs):
        """Run the specified calculations on the progenitors of this halo

        For more information see calculate_for_descendants.
        """
        from .. import relation_finding
        kwargs['strategy'] = relation_finding.MultiHopMajorProgenitorsStrategy
        return self.calculate_for_descendants(*plist, **kwargs)

    def reverse_property_cascade(self, *args, **kwargs):
        """The old alias for calculate_for_progenitors, retained for compatibility"""
        return self.calculate_for_progenitors(*args, **kwargs)

    def property_cascade(self, *args, **kwargs):
        """The old alias for calculate_for_descendants, retained for compatibility"""
        return self.calculate_for_descendants(*args, **kwargs)


    @property
    def next(self):
        if not hasattr(self, '_next'):
            from .. import relation_finding
            strategy = relation_finding.HopMajorDescendantStrategy(self)
            self._next=strategy.first()

        return self._next

    @property
    def previous(self):
        if not hasattr(self, '_previous'):
            from .. import relation_finding
            strategy = relation_finding.HopMajorProgenitorStrategy(self)
            self._previous=strategy.first()

        return self._previous

    def short(self):
        return "<Halo " + str(self.halo_number) + " of ...>"

class Halo(SimulationObjectBase):
    __mapper_args__ = {
        'polymorphic_identity': 0
    }

    tag = "halo"

class Tracker(SimulationObjectBase):
    __mapper_args__ = {
        'polymorphic_identity':3
    }

    tag = "tracker"

    def __init__(self, timestep, halo_number):
        super().__init__(timestep, halo_number, halo_number, halo_number, 0,0,0,
                                 self.__mapper_args__['polymorphic_identity'])

    @property
    def tracker(self):
        return self.timestep.simulation.trackers.filter_by(halo_number=self.halo_number).first()

    def load(self, mode=None):
        handler = self.timestep.simulation.get_output_handler()
        return handler.load_tracked_region(self.timestep.extension, self.tracker, mode=mode)


class BH(Tracker):
    __mapper_args__ = {
        'polymorphic_identity': 1
    }

    tag = "BH"


class Group(SimulationObjectBase):
    __mapper_args__ = {
        'polymorphic_identity':2
    }

    tag = "group"

    def __init__(self, *args):
        super().__init__(*args)
        self.object_typecode = 2




class PhantomHalo(SimulationObjectBase):
    __mapper_args__ = {
        'polymorphic_identity': 4
    }

    tag = "phantom"

    def __init__(self, timestep, halo_number, finder_id):
        super().__init__(timestep, halo_number, finder_id, finder_id, 0,0,0,
                                 self.__mapper_args__['polymorphic_identity'])


TimeStep.halos = orm.relationship(Halo, lazy='dynamic',viewonly=True,
                                  order_by=SimulationObjectBase.halo_number)
TimeStep.trackers = orm.relationship(Tracker,lazy='dynamic',viewonly=True,
                                  order_by=Tracker.halo_number)
TimeStep.bhs = orm.relationship(BH, lazy='dynamic',viewonly=True,
                                  order_by=BH.halo_number)
TimeStep.groups = orm.relationship(Group, lazy='dynamic',viewonly=True,
                                  order_by=Group.halo_number)
TimeStep.phantoms = orm.relationship(PhantomHalo, lazy='dynamic',viewonly=True,
                                  order_by=PhantomHalo.halo_number)
