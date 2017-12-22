from .. import LiveHaloProperties
import numpy as np

class BHGal(LiveHaloProperties):
    def __init__(self, simulation=None, choose='BH_mass', minmax='max', bhtype='BH_central'):
        super(BHGal, self).__init__(simulation)
        self._maxmin = minmax
        self._choicep = choose
        self._bhtype = bhtype

    names = 'bh'

    def requires_property(self):
        return self._bhtype, self._bhtype+"."+self._choicep

    def live_calculate(self, halo, *args):
        if halo.object_typecode != 0:
            return None

        if self._bhtype not in list(halo.keys()):
            return None

        if type(halo[self._bhtype]) is list:
            chp = [bh[self._choicep] for bh in halo[self._bhtype] if self._choicep in bh]
            if len(chp) == 0:
                return None
            target = None
            if self._maxmin == 'min':
                target = np.argmin(chp)
            if self._maxmin == 'max':
                target = np.argmax(chp)
            if target is None:
                return None
            return halo[self._bhtype][target]
        else:
            return halo[self._bhtype]

class BHCentral(BHGal):
    names = 'bhcenter'

    def __init__(self, simulation=None):
        super(BHCentral, self).__init__(simulation, 'BH_central_distance', 'min', 'BH_central')
