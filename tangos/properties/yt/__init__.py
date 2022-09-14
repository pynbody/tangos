from ...input_handlers import yt as yt_handler_module
from .. import PropertyCalculation


class YtPropertyCalculation(PropertyCalculation):
    works_with_handler = yt_handler_module.YtInputHandler
    requires_particle_data = True

from . import DM, basic, gas, stars
