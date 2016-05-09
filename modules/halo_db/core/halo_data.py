from sqlalchemy import Column, Integer, ForeignKey, Float, LargeBinary, Boolean
from sqlalchemy.orm import relationship, backref

from . import Base
from .halo import Halo
from .dictionary import DictionaryItem
from .creator import Creator
from .timestep import TimeStep

from .. import data_attribute_mapper

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
        from . import current_creator

        if isinstance(halo, Halo):
            self.halo = halo
        else:
            self.halo_id = halo

        self.name = name
        self.data = data
        self.creator = current_creator

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
        try:
            cls = self.name.providing_class()
        except NameError:
            cls = None

        if hasattr(cls, 'reassemble'):
            return cls.reassemble(self, *options)
        else:
            return self.data_raw

    def x_values(self):
        if not self.data_is_array():
            raise ValueError, "The data is not an array"
        return self.name.providing_class()(self.halo.timestep.simulation).plot_x_values(self.data)

    def plot(self, *args, **kwargs):
        xdat = self.x_values()
        import matplotlib.pyplot as p
        return p.plot(xdat, self.data, *args, **kwargs)

    @data.setter
    def data(self, data):
        data_attribute_mapper.set_data_of_unknown_type(self, data)


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
        from . import current_creator

        self.halo_from = halo_from
        self.halo_to = halo_to

        self.relation = relationship
        self.weight = weight

        self.creator = current_creator

    def __repr__(self):
        return "<HaloLink " + str(self.relation.text) + " " + str(self.halo_from) + " " + str(self.halo_to) + ">"





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
Halo.all_reverse_links = relationship(HaloLink, primaryjoin=(HaloLink.halo_to_id == Halo.id))
