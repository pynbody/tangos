from __future__ import absolute_import
from sqlalchemy import Column, Integer, ForeignKey, String, Float, Boolean
from sqlalchemy.orm import relationship, backref

from . import Base, DictionaryItem


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