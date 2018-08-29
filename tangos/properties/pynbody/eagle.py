from . import PynbodyPropertyCalculation
from ...input_handlers import eagle
from ...log import logger
import numpy as np

class EagleBH(PynbodyPropertyCalculation):
    works_with_handler = eagle.EagleLikeInputHandler

    names = "BH_mass", "BH_mdot"

    def calculate(self, particle_data, halo_entry):
        return particle_data.bh['BH_Mass'].sum(), particle_data.bh['BH_Mdot'].sum()
