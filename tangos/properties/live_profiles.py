# Live properties suitable for calculations on underlying profiles, e.g. density profiles, mass profiles etc

from . import LivePropertyCalculation
import numpy as np

class AtPosition(LivePropertyCalculation):
    def __init__(self, simulation, position, array):
        super(AtPosition, self).__init__(simulation)
        self._array_info = array

    names = "at"

    def live_calculate(self, halo, pos, ar):
        x0 = self._array_info.plot_x0()
        delta_x = self._array_info.plot_xdelta()

        # linear interpolation
        i0 = int((pos-x0)/delta_x)
        i1 = i0+1

        i0_loc = float(i0)*delta_x+x0
        i1_weight = (pos-i0_loc)/delta_x
        i0_weight = 1.0-i1_weight

        if i1>=len(ar) or i0<0:
            return None
        else:
            return ar[i0]*i0_weight + ar[i1]*i1_weight

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
