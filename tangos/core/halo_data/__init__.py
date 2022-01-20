from __future__ import absolute_import
from sqlalchemy.orm import relationship



from .property import HaloProperty
from .link import HaloLink



def _initialise_halo_property_relationships():

    from ..halo import SimulationObjectBase
    from ..timestep import TimeStep

    SimulationObjectBase.properties = relationship(HaloProperty, cascade='all', lazy='dynamic',
                                   primaryjoin=(HaloProperty.halo_id == SimulationObjectBase.id) & (
                                       HaloProperty.deprecated == False),
                                   order_by=HaloProperty.id,
                                   uselist=True,overlaps='deprecated_properties')

    SimulationObjectBase.deprecated_properties = relationship(HaloProperty, cascade='all', lazy='dynamic',
                                              primaryjoin=(HaloProperty.halo_id == SimulationObjectBase.id) & (
                                                  HaloProperty.deprecated == True),
                                              order_by=HaloProperty.id,
                                              uselist=True,overlaps='properties')

    TimeStep.links_from = relationship(HaloLink, secondary=SimulationObjectBase.__table__,
                                       secondaryjoin=(
                                           HaloLink.halo_from_id == SimulationObjectBase.id),
                                       primaryjoin=(
                                           SimulationObjectBase.timestep_id == TimeStep.id),
                                       cascade='none', lazy='dynamic',
                                       order_by=HaloLink.id,
                                       viewonly=True)



    TimeStep.links_to = relationship(HaloLink, secondary=SimulationObjectBase.__table__,
                                     secondaryjoin=(
                                         HaloLink.halo_to_id == SimulationObjectBase.id),
                                     primaryjoin=(SimulationObjectBase.timestep_id == TimeStep.id),
                                     cascade='none', lazy='dynamic',
                                     order_by=HaloLink.id,
                                     viewonly=True)



    SimulationObjectBase.all_links = relationship(HaloLink, primaryjoin=(HaloLink.halo_from_id == SimulationObjectBase.id),
                                  viewonly=True, order_by=HaloLink.id)
    SimulationObjectBase.all_reverse_links = relationship(HaloLink, primaryjoin=(HaloLink.halo_to_id == SimulationObjectBase.id),
                                          viewonly=True, order_by=HaloLink.id)

_initialise_halo_property_relationships()