from __future__ import absolute_import
from sqlalchemy import Column, Integer, ForeignKey, Float, LargeBinary, Boolean
from sqlalchemy.orm import relationship, backref, deferred

from .. import data_attribute_mapper, extraction_patterns
from .. import Base
from .. import creator
from ..dictionary import DictionaryItem
from ..halo import Halo


class HaloProperty(Base):
    __tablename__ = 'haloproperties'

    id = Column(Integer, primary_key=True)
    halo_id = Column(Integer, ForeignKey('halos.id'))
    # n.b. backref defined below
    halo = relationship(Halo, cascade='none', backref=backref('all_properties'))

    data_float = Column(Float)
    data_array = deferred(Column(LargeBinary), group='data')
    data_int = Column(Integer)

    name_id = Column(Integer, ForeignKey('dictionary.id'))
    name = relationship(DictionaryItem)

    deprecated = Column(Boolean, default=False, nullable=False)

    creator = relationship(creator.Creator, backref=backref(
        'properties', cascade='all', lazy='dynamic'), cascade='save-update')
    creator_id = Column(Integer, ForeignKey('creators.id'))

    def __init__(self, halo, name, data):
        if isinstance(halo, Halo):
            self.halo = halo
        else:
            self.halo_id = halo

        self.name = name
        self.data = data
        self.creator_id = creator.get_creator_id()

    def __repr__(self):
        if self.deprecated:
            x = "<HaloProperty (deprecated) "
        else:
            x = "<HaloProperty "
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
        return self.get_data_with_reassembly_options()

    def get_data_with_reassembly_options(self, *options):
        return extraction_patterns.HaloPropertyValueWithReassemblyOptionsGetter(*options).postprocess_data_objects([self])[0]


    @property
    def description(self):
        return self.name.providing_class(type(self.halo.handler))(self.halo.timestep.simulation)

    def x_values(self):
        if not self.data_is_array():
            raise ValueError("The data is not an array")
        return self.description.plot_x_values(self.data)

    @data.setter
    def data(self, data):
        data_attribute_mapper.set_data_of_unknown_type(self, data)


