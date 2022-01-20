from __future__ import absolute_import
from sqlalchemy.orm import relationship



from .property import HaloProperty
from .link import HaloLink



def _initialise_halo_property_relationships():

    from ..halo import Halo
    from ..timestep import TimeStep

    Halo.properties = relationship(HaloProperty, cascade='all', lazy='dynamic',
                                   primaryjoin=(HaloProperty.halo_id == Halo.id) & (
                                       HaloProperty.deprecated == False),
                                   order_by=HaloProperty.id,
                                   uselist=True)


    Halo.deprecated_properties = relationship(HaloProperty, cascade='all',
                                              primaryjoin=(HaloProperty.halo_id == Halo.id) & (
                                                  HaloProperty.deprecated == True),
                                              order_by=HaloProperty.id,
                                              uselist=True)

    TimeStep.links_from = relationship(HaloLink, secondary=Halo.__table__,
                                       secondaryjoin=(
                                           HaloLink.halo_from_id == Halo.id),
                                       primaryjoin=(
                                           Halo.timestep_id == TimeStep.id),
                                       cascade='none', lazy='dynamic',
                                       order_by=HaloProperty.id,
                                       viewonly=True)



    TimeStep.links_to = relationship(HaloLink, secondary=Halo.__table__,
                                     secondaryjoin=(
                                         HaloLink.halo_to_id == Halo.id),
                                     primaryjoin=(Halo.timestep_id == TimeStep.id),
                                     cascade='none', lazy='dynamic',
                                     order_by=HaloProperty.id,
                                     viewonly=True)



    Halo.all_links = relationship(HaloLink, primaryjoin=(HaloLink.halo_from_id == Halo.id),
                                  viewonly=True, order_by=HaloLink.id)
    Halo.all_reverse_links = relationship(HaloLink, primaryjoin=(HaloLink.halo_to_id == Halo.id),
                                          viewonly=True, order_by=HaloLink.id)

_initialise_halo_property_relationships()