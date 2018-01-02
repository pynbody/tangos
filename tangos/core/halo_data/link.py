from __future__ import absolute_import
from sqlalchemy import Column, Integer, ForeignKey, Float, LargeBinary, Boolean
from sqlalchemy.orm import relationship, backref

from .. import Base
from .. import creator
from ..dictionary import DictionaryItem
from ..halo import Halo


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
        return "<HaloLink " + str(self.relation.text) + " " + str(self.halo_from.path) + " to " + str(self.halo_to.path) \
               + " weight=%.2f>"%self.weight

