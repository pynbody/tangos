from . import HaloProperties
import numpy as np
import math
import pynbody
import time
import copy

@pynbody.analysis.profile.Profile.profile_property
def j_HI(self):
    """
    Magnitude of the total angular momentum in HI as a function of distance from halo center
    """
    j_HI = np.zeros(self.nbins)

    for i in range(self.nbins):
        subs = self.sim[self.binind[i]]
        jx = (subs['j'][:, 0] * subs['mass'] * subs['HI']).sum() / (self['mass'][i] * self['HI'][i])
        jy = (subs['j'][:, 1] * subs['mass'] * subs['HI']).sum() / (self['mass'][i] * self['HI'][i])
        jz = (subs['j'][:, 2] * subs['mass'] * subs['HI']).sum() / (self['mass'][i] * self['HI'][i])

        j_HI[i] = np.sqrt(jx ** 2 + jy ** 2 + jz ** 2)

    return j_HI


class AngMomHI(HaloProperties):

    def name(self):
        return "j_HI", "J_HI_profile"

    def requires_property(self):
        return ['SSC', 'Rvir']

    def rstat(self, halo, maxrad, cen, delta=0.1):

        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)

        pro = pynbody.analysis.profile.Profile(halo.g, type='lin', ndim=3,
                                               min=0, max=maxrad, nbins=nbins)

        j_HI_a = pro['j_HI']
		j_HI_rb = pro['rbins']
        j_HI_a = np.array(j_HI_a)

        return j_HI_a, j_HI_rb

    def calculate(self,  halo, properties):
        if len(halo.g > 100):
            com = properties['SSC']
            rad = properties['Rvir']
            halo["pos"] -= com
            halo.wrap()
            vcen = pynbody.analysis.halo.vel_center(halo,cen_size="1 kpc",retcen=True)
            halo['vel'] -= vcen

            delta = existing_properties.get('delta',0.1)
            j_HI_pro = rstat(self, halo, rad, com, delta)

            HImass = halo.g['mass']*halo.g['HI']
            mvec = pynbody.array.SimArray(np.transpose(np.vstack((HImass,HImass,HImass))),'Msol')
            jvec = np.sum(halo.g['j'] * mvec, axis=0)/np.sum(HImass)
            j_HI_tot = np.sqrt(np.sum(jvec**2))

            halo["pos"] += com
            halo['vel'] += vcen
            halo.wrap()

            return j_HI_tot, j_HI_pro
        else:
            return 0, (np.array([0]),np.array([0]))



