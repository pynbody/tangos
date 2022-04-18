# Live properties suitable for calculations on underlying profiles, e.g. density profiles, mass profiles etc

from . import LivePropertyCalculation
import numpy as np

class AtPosition(LivePropertyCalculation):
    def __init__(self, simulation, position, array):
        super(AtPosition, self).__init__(simulation)
        self._array_info = array

    names = "at"

    def live_calculate(self, halo, pos, ar):
        return self._array_info.get_interpolated_value(pos, ar)



class MaxMinProperty(LivePropertyCalculation):
    def __init__(self, simulation, array):
        super(MaxMinProperty, self).__init__(simulation)
        self._array_info = array

    names = "max", "min", "posmax", "posmin"

    def live_calculate(self, halo, array):
        max_, min_ = np.max(array), np.min(array)
        amax, amin = np.argmax(array), np.argmin(array)
        index_to_r = lambda index: index*self._array_info.plot_xdelta()+self._array_info.plot_x0()
        return float(max_), float(min_), index_to_r(amax), index_to_r(amin)
