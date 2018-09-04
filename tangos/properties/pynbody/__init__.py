from .. import PropertyCalculation
from ...input_handlers import pynbody as pynbody_handler_module

class PynbodyPropertyCalculation(PropertyCalculation):
    works_with_handler = pynbody_handler_module.PynbodyInputHandler
    requires_particle_data = True

PynbodyHaloProperties = PynbodyPropertyCalculation # old name, to be deprecated

from . import BH, SF, zoom, centring, profile, images, gas, eagle, mass