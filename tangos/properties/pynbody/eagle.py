from . import PynbodyPropertyCalculation
from ...input_handlers import eagle
from ...log import logger
import numpy as np

class EagleParentFinder(PynbodyPropertyCalculation):
    works_with_handler = eagle.EagleLikeInputHandler

    names = "parent", "original_subgroup_id"

    def preloop(self, sim, db_timestep):
        # generate map from original group number to tangos DB object
        self._groups_objs = {}
        for o in db_timestep.groups:
            self._groups_objs[o.finder_id] = o

    def calculate(self, particle_data, halo_entry):
        subhalo = particle_data['SubGroupNumber'][0]
        parent = particle_data['GroupNumber'][0]
        parent_obj = self._groups_objs[parent]
        return parent_obj, subhalo

class EagleChildFinder(PynbodyPropertyCalculation):
    works_with_handler = eagle.EagleLikeInputHandler
    requires_particle_data = False
    names = "child"


    def preloop(self, sim, db_timestep):
        # generate map from original halo number to tangos DB object
        self._halos_objs = {}
        for o in db_timestep.halos:
            self._halos_objs[o.finder_id] = o

    def calculate(self, particle_data, halo_entry):
        subhalo = particle_data['SubGroupNumber'][0]
        parent = particle_data['GroupNumber'][0]
        parent_obj = self._groups_objs[parent]
        return parent_obj, subhalo