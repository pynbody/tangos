from __future__ import absolute_import
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import relationship, backref

from .. import Base
from .. import creator
from ..dictionary import DictionaryItem
from ..halo import SimulationObjectBase
from ...config import DOUBLE_PRECISION


class HaloLink(Base):
    __tablename__ = 'halolink'

    id = Column(Integer, primary_key=True)
    halo_from_id = Column(Integer, ForeignKey('halos.id'))
    halo_to_id = Column(Integer, ForeignKey('halos.id'))

    halo_from = relationship(SimulationObjectBase, primaryjoin=halo_from_id == SimulationObjectBase.id,
                             backref=backref('links', cascade='all',
                                             lazy='dynamic',
                                             primaryjoin=halo_from_id == SimulationObjectBase.id),
                             cascade='')

    halo_to = relationship(SimulationObjectBase, primaryjoin=(halo_to_id == SimulationObjectBase.id),
                           backref=backref('reverse_links', cascade='all, delete-orphan',
                                           lazy='dynamic',
                                           primaryjoin=halo_to_id == SimulationObjectBase.id),
                           cascade='')

    weight = Column(DOUBLE_PRECISION)

    creator_id = Column(Integer, ForeignKey('creators.id'))
    creator = relationship(creator.Creator, backref=backref(
        'halolinks', cascade='all, delete', lazy='dynamic'), cascade='save-update')

    relation_id = Column(Integer, ForeignKey('dictionary.id'))
    relation = relationship(DictionaryItem, primaryjoin=(
        relation_id == DictionaryItem.id), cascade='save-update,merge')



    def __init__(self,  halo_from, halo_to, relationship, weight=1.0):
        self.halo_from = halo_from
        self.halo_to = halo_to

        self.relation = relationship
        self.weight = weight

        self.creator_id = creator.get_creator_id()

    def __repr__(self):
        if self.weight is None:
            weight_str = "None"
        else:
            weight_str = "%.2f"%self.weight
        return "<HaloLink " + str(self.relation.text) + " " + str(self.halo_from.path) + " to " + str(self.halo_to.path) \
               + " weight=%s>"%weight_str
