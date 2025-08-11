from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import Session, backref, relationship

from ...config import DOUBLE_PRECISION
from .. import Base, creator
from ..dictionary import DictionaryItem
from ..halo import SimulationObjectBase


class HaloLink(Base):
    __tablename__ = 'halolink'

    id = Column(Integer, primary_key=True)
    halo_from_id = Column(Integer, ForeignKey('halos.id'))
    halo_to_id = Column(Integer, ForeignKey('halos.id'))

    halo_from = relationship(SimulationObjectBase, primaryjoin=halo_from_id == SimulationObjectBase.id,
                             backref=backref('links', cascade_backrefs=False,
                                             lazy='dynamic', viewonly=True,
                                             primaryjoin=halo_from_id == SimulationObjectBase.id),
                             cascade='')

    halo_to = relationship(SimulationObjectBase, primaryjoin=(halo_to_id == SimulationObjectBase.id),
                           backref=backref('reverse_links', cascade_backrefs=False,
                                           lazy='dynamic', viewonly=True,
                                           primaryjoin=halo_to_id == SimulationObjectBase.id),
                           cascade='')

    weight = Column(DOUBLE_PRECISION)

    creator_id = Column(Integer, ForeignKey('creators.id'))
    creator = relationship(creator.Creator, backref=backref(
        'halolinks', cascade_backrefs=False, lazy='dynamic'), cascade='save-update')

    relation_id = Column(Integer, ForeignKey('dictionary.id'))
    relation = relationship(DictionaryItem, primaryjoin=(
        relation_id == DictionaryItem.id), cascade='')



    def __init__(self,  halo_from, halo_to, relationship, weight=1.0):
        if isinstance(halo_from, SimulationObjectBase):
            self.halo_from = halo_from
        else:
            self.halo_from_id = halo_from

        if isinstance(halo_to, SimulationObjectBase):
            self.halo_to = halo_to
        else:
            self.halo_to_id = halo_to

        if isinstance(relationship, DictionaryItem):
            self.relation = relationship
        else:
            self.relation_id = relationship

        self.weight = weight

        self.creator_id = creator.get_creator_id()

    def __repr__(self):
        if self.weight is None:
            weight_str = "None"
        else:
            weight_str = "%.2f"%self.weight
        return "<HaloLink " + str(self.relation.text) + " " + str(self.halo_from.path) + " to " + str(self.halo_to.path) \
               + " weight=%s>"%weight_str
