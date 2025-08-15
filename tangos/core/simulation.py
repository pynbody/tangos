import datetime

import numpy as np
from sqlalchemy import Column, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import Session, backref, relationship

from .. import config, input_handlers
from ..config import DOUBLE_PRECISION, LARGE_BINARY
from . import Base, creator, data_attribute_mapper
from .dictionary import DictionaryItem, get_dict_id, get_or_create_dictionary_item


class Simulation(Base):
    __tablename__ = 'simulations'
    # __table_args__ = {'useexisting': True}

    id = Column(Integer, primary_key=True)
    basename = Column(Text)
    creator = relationship(
        creator.Creator, backref=backref('simulations', cascade_backrefs=False), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    def __init__(self, basename):
        self.basename = basename
        self.creator_id = creator.get_creator().id

    def __repr__(self):
        return "<Simulation(\"" + self.basename + "\")>"

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def get_output_handler(self):
        """Get a HandlerBase object, pre-configured suitably for this simulation

        :rtype simulation_outputs.HandlerBase"""
        if not hasattr(self, "_handler"):
            handler_class = self.output_handler_class
            self._handler = handler_class(self.basename)
        return self._handler

    @property
    def output_handler_class(self):
        if not hasattr(self, "_handler_class"):
            self._handler_class = input_handlers.get_named_handler_class(self.get("handler", config.default_fileset_handler_class))
        return self._handler_class

    def keys(self):
        return [prop.name.text for prop in self.properties.all()]

    def cache_properties(self):
        """Cache the properties of this simulation in a dictionary for faster/disconnected access."""
        self._properties_cache = {prop.name.text: prop.data for prop in self.properties.all()}

    def __contains__(self, item):
        return item in list(self.keys())

    def __getitem__(self, i):
        from . import Session
        if isinstance(i, int):
            return self.timesteps[i]
        else:
            return self._get_sim_property(i)

        raise KeyError(i)

    def _get_sim_property(self, name):
        if hasattr(self, "_properties_cache"):
            return self._get_sim_property_from_cache(name)
        else:
            return self._get_sim_property_from_database(name)

    def __raise_key_error(self, name):
        raise KeyError(f"Simulation property '{name}' not found")

    def _get_sim_property_from_cache(self, name):
        if name not in self._properties_cache:
            self.__raise_key_error(name)
        return self._properties_cache[name]

    def _get_sim_property_from_database(self, name):
        session = Session.object_session(self)
        did = get_dict_id(name, session=session)
        result = session.query(SimulationProperty).filter_by(name_id=did,
                                                           simulation_id=self.id).first()
        if result is None:
            self.__raise_key_error(name)
        return result.data

    def __setitem__(self, st, data):
        from . import Session
        assert isinstance(st, str)
        session = Session.object_session(self)
        name = get_or_create_dictionary_item(session, st)
        propobj = self.properties.filter_by(name_id=name.id).first()
        if propobj is None:
            propobj = SimulationProperty(self, name, data)
            session.add(propobj)

        propobj.data = data
        session.commit()

    @property
    def path(self):
        return self.basename

    @property
    def escaped_basename(self):
        return self.basename.replace("/","_")


class SimulationProperty(Base):
    __tablename__ = 'simulationproperties'

    id = Column(Integer, primary_key=True)
    name_id = Column(Integer, ForeignKey('dictionary.id'))
    name = relationship(DictionaryItem)

    simulation_id = Column(Integer, ForeignKey('simulations.id'))
    simulation = relationship(Simulation, backref=backref('properties', cascade_backrefs=False,
                                                          lazy='dynamic', order_by=name_id, cascade=''), cascade='delete')

    creator_id = Column(Integer, ForeignKey('creators.id'))
    creator = relationship(creator.Creator, backref=backref(
        'simproperties', cascade_backrefs=False, lazy='dynamic', cascade=''), cascade='save-update', cascade_backrefs=False)

    data_float = Column(DOUBLE_PRECISION)
    data_int = Column(Integer)
    data_time = Column(DateTime)
    data_string = Column(Text)
    data_array = Column(LARGE_BINARY)


    def __init__(self, sim, name, data):
        session = Session.object_session(sim)
        self.simulation = sim
        if not isinstance(name, DictionaryItem):
            name = get_or_create_dictionary_item(session, name)
        self.name = name
        self.data = data
        self.creator = creator.get_creator(session)
        assert session == Session.object_session(self.creator)

    def data_repr(self):
        f = self.data
        if type(f) is float:
            x = "%.2g" % f
        elif type(f) is datetime.datetime:
            x = f.strftime('%H:%M %d/%m/%y')
        elif type(f) is str or type(f) is str:
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
