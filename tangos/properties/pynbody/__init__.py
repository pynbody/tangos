from ...input_handlers import pynbody as pynbody_handler_module
from .. import PropertyCalculation


class PynbodyPropertyCalculation(PropertyCalculation):
    works_with_handler = pynbody_handler_module.PynbodyInputHandler
    requires_particle_data = True

PynbodyHaloProperties = PynbodyPropertyCalculation # old name, to be deprecated

from . import BH, SF, centring, eagle, gas, images, mass, profile, radius, zoom
