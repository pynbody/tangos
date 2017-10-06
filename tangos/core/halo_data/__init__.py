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

_initialise_halo_property_relationships()