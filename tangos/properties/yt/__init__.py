from .. import PropertyCalculation
from ...input_handlers import yt as yt_handler_module

class YtPropertyCalculation(PropertyCalculation):
    works_with_handler = yt_handler_module.YtInputHandler
    requires_particle_data = True

from . import basic, DM, gas, stars
