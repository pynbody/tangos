from .. import HaloProperties
from ...simulation_output_handlers import pynbody as pynbody_handler_module

class PynbodyHaloProperties(HaloProperties):
    works_with_handler = pynbody_handler_module.PynbodyOutputSetHandler

from . import BH, zoom, centring, profile, images, gas