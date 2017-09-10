from . import HaloProperties
import numpy as np
import pynbody


lamcen = {'u':0.365,'b':0.445,'v':0.551,'r':0.658,'i':0.806,'j':1.22,'h':1.63,'k':2.19}

class DustAttenuation(HaloProperties):
    @classmethod
    def name(self):
        return "dustExt_V", "dustExt_B", "dustExt_K", "dustExt_U", "dustExt_J", "dustExt_I"

    def requires_property(self):
        return ["SSC", "Rvir"]

    def k(self,lam,Rv):
        if lam >= 0.63 and lam <= 2.2:
            return 2.659*(1.04/lam - 1.857) + Rv
        if lam >= 0.12 and lam < 0.63:
            return 2.659*(0.011/lam**3 - 0.198/lam**2 + 1.509/lam - 2.156) + Rv
        if lam < 0.12 or lam > 2.2:
            raise ValueError, "wavelength not in acceptable range"

    def calculate(self, halo, properties):
        if len(halo.gas) < 5:
            return 0, 0, 0, 0, 0, 0
        Rv = 4.0
        A1600max = 2.0
        dustf = 0.01  #Draine 2007
        a = pynbody.array.SimArray(0.1,'1e-6 m') #Todini+Ferrarra (2001), Nozawa+ (2003)
        rho = pynbody.array.SimArray(2.5,'g cm**-3')

        Md = np.sum(halo.gas['mass'] * (halo.gas['metals']/0.02) * halo.gas['HI'] * dustf)

        com = properties['SSC']
        rad = properties['Rvir']
        halo["pos"] -= com
        halo.wrap()

        rord = np.argsort(halo.gas['r'])
        Msum = np.cumsum(halo.gas['mass'][rord]*(halo.g['metals'][rord]/0.02)*halo.gas['HI'][rord]*dustf)
        xx, = np.where(Msum >= Md*0.5)
        Rhalf = halo.gas['r'][rord[xx]][0]

        sigD = 0.5*Md/(np.pi*Rhalf**2)
        tau = 3.*sigD/(4.*a.in_units('kpc')*rho.in_units('Msol kpc**-3'))
        A1600 = 1.086*tau
        if A1600 > A1600max: A1600=A1600max
        EBV = A1600/self.k(0.16,Rv)

        dustExt = {'u': 0, 'b': 0, 'v': 0, 'i': 0, 'j': 0, 'k': 0}
        for key in dustExt.keys():
            dustExt[key] = self.k(lamcen[key],Rv)*EBV

        return dustExt['v'], dustExt['b'], dustExt['k'], dustExt['u'], dustExt['j'], dustExt['i']





