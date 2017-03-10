from . import HaloProperties
import numpy as np
import math
import pynbody
import time
import copy

class BasicHaloProperties(HaloProperties):

    def preloop(self, sim, filename, property_array):
        self.f = sim


    def determine_sub(self, current_sc, maxi):
        q = 0
        for i in self.proplist[:maxi]:
            if i.has_key("SSC") and i["SSC"] != None:
                q += 1
                diff = (
                    (np.subtract(i["SSC"], current_sc) ** 2).sum() ** 0.5) / i["Rvir"]
                if diff < 1:
                    return q
        return 0

    @classmethod
    def name(self):
        return "SSC", "Rvir", "Mvir", "Mgas", "Mbar", "Mstar"

    def requires_property(self):
        return []

    def calculate(self, halo, existing_properties):
        gi = halo

        # time the loading of data...
        self.mark_timer('fload-S')
        halo['x']
        self.mark_timer('fload-F')

        if len(gi) > 100:
            giX = gi.dm

            pmin = copy.copy(giX['pos'][0])
            print "initial",self.f['pos'][0]
            self.f['pos']-=pmin
            self.f.wrap()

            self.mark_timer('ssc-S')
            com = pynbody.analysis.halo.shrink_sphere_center(
                gi,shrink_factor=0.8)
            self.mark_timer('ssc-F')

            if any(com != com):
                raise RuntimeError, "Something bizarre has happened with the centering"

            self.f["pos"] -= com

            self.f.gas['mass']
            self.f.star['mass']
            self.f.dm['mass']

            self.mark_timer('vrad-S')
            try:
                rad = pynbody.analysis.halo.virial_radius(self.f, r_max=3500)
            except ValueError:
                # assume an isolated run
                rad = self.f.properties['boxsize'].in_units('kpc') / 2

            self.mark_timer('vrad-F')

            sub = self.f[pynbody.filt.Sphere(rad)]

            mgas = sub.gas["mass"].sum()
            mstar = sub.star["mass"].sum()

            com+=pmin

            self.f["pos"] += com
            self.f.wrap()


            print "x-check [matches above?]", self.f['pos'][0]
            return com.view(np.ndarray), rad, sub["mass"].sum(), mgas, mgas + mstar, mstar

        else:
            return None, 0, 0, 0, 0

class Vcom(HaloProperties):
    @classmethod
    def name(self):
        return "Vcom"
    def calculate(self, halo, properties):
        return pynbody.analysis.halo.center_of_mass_velocity(halo)


class SSCComoving(HaloProperties):
    @classmethod
    def name(self):
        return "SSC_comoving"

    @classmethod
    def requires_simdata(self):
        return False

    @classmethod
    def no_proxies(self):
        return True

    def calculate(self, halo, properties):
        return properties['SSC']*(1.+properties.timestep.redshift)

class MColdGas(HaloProperties):

    def calculate(self, halo, exist):
        if len(halo.gas)==0:
            return 0, 0
        coldgas = halo.gas[pynbody.filt.LowPass("temp", 2.e4)]["mass"].sum()
        higas = (halo.gas["mass"] * halo.gas["HI"]).sum()
        return coldgas, higas

    @classmethod
    def name(self):
        return "MColdGas", "MHIGas"

@pynbody.analysis.profile.Profile.profile_property
def HI(self):
    '''
    profile of total HI fraction
    '''

    HIfrac = np.zeros(self.nbins)
    for i in range(self.nbins):
        subs = self.sim[self.binind[i]]
        if len(subs)>0:
            HIfrac[i] = (subs['mass']*subs['HI']).sum()/subs['mass'].sum()
        else:
            HIfrac[i] = 0.0
    return HIfrac

class MassEnclosed(HaloProperties):
    @classmethod
    def name(self):
        return "StarMass_encl", "GasMass_encl", "HIMass_encl", "ColdGasMass_encl"

    def requires_property(self):
        return "SSC", "Rvir"

    def rstat(self, halo, maxrad, delta=0.1):
        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)
        proS = pynbody.analysis.profile.Profile(halo.s, type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
        proG = pynbody.analysis.profile.Profile(halo.g, type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
        if len(halo.g)>0:
            proCG = pynbody.analysis.profile.Profile(halo.g[pynbody.filt.LowPass("temp", 2.e4)],
                                                 type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
        else:
            proCG = proG

        return proS['mass_enc'],proG['mass_enc'], np.cumsum(proG['mass']*proG['HI']), proCG['mass_enc']

    def calculate(self,halo,properties):
        com = properties['SSC']
        rad = properties['Rvir']
        halo.s["pos"] -= com
        halo.g["pos"] -= com
        halo.s.wrap()
        halo.g.wrap()
        delta = properties.get('delta',0.1)

        starM, gasM, HIM, coldM = self.rstat(halo,rad,delta)

        halo.s["pos"] += com
        halo.g["pos"] += com
        halo.s.wrap()
        halo.g.wrap()

        return starM, gasM, HIM, coldM

    def plot_x0(cls):
        return 0.0

    def plot_xdelta(cls):
        return 0.1


class Contamination(HaloProperties):

    def calculate(self, halo, exist):
        n_heavy = (halo.dm['mass'] > halo.ancestor.dm['mass'].min()).sum()
        return float(n_heavy) / len(halo.dm)

    @classmethod
    def name(self):
        return "contamination_fraction"


class HaloVvir(HaloProperties):

    @classmethod
    def name(self):
        return "Vvir"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return "Mvir", "Rvir"

    def calculate(self, halo, properties):
        _G = 4.33227e-6
        return math.sqrt(_G * properties["Mvir"] / properties["Rvir"])

class HaloMDM(HaloProperties):
    @classmethod
    def name(cls):
        return "MDM"

    @classmethod
    def requires_simdata(self):
        return False

    def calculate(self, halo, properties):
        return properties['Mvir']-properties['Mbar']


class HaloRmax(HaloProperties):

    @classmethod
    def name(self):
        return "Rmax"

    def requires_property(self):
        return ["SSC"]

    def calculate(self, halo, properties):
        halo["pos"] -= properties["SSC"]
        rmx = pynbody.derived.r(halo).max()
        halo["pos"] += properties["SSC"]
        return rmx


class Softening(HaloProperties):

    @classmethod
    def name(self):
        return "eps", "epsmin", "epsmax"

    def calculate(self, halo, existing_properties):
        return halo.mean_by_mass("eps"), halo["eps"].min(), halo["eps"].max()




class Metallicity(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "mean_gas_metal", "mean_hi_metal"

    def calculate(self, halo, existing_properties):
        gas = halo[siman.ParticleTypeFilter(siman.Particle.gas)]
        try:
            hi_met = (gas["metal"] * (1. - gas["nHII"]) * gas["mass"]
                      ).sum() / ((1. - gas["nHII"]) * gas["mass"]).sum()
        except ZeroDivisionError:
            hi_met = 0.
        return gas["metal"].mean_by_mass(), hi_met


class Temperature(HaloProperties):

    @classmethod
    def name(self):
        return "temp"

    def calculate(self, halo, existing_properties):
        return halo["temp"].mean_by_mass()


class Magnitudes(HaloProperties):
    # include

    def calculate(self, halo, existing_properties):
        hm = pynbody.analysis.luminosity.halo_mag
        ok, = np.where(halo.s['tform']>0)
        return hm(halo.s[ok], "v"), hm(halo.s[ok], "b"), hm(halo.s[ok], "k"), hm(halo.s[ok], "u"), hm(halo.s[ok], "j"), hm(halo.s[ok], "i")

    @classmethod
    def name(self):
        return "V", "B", "K", "U", "J", "I"


class ABMagnitudes(HaloProperties):

    def calculate(self, halo, existing_properties):
        hm = pynbody.analysis.luminosity.halo_mag
        ok, = np.where(halo.s['tform']>0)
        ABcorr = {'u':0.79,'b':-0.09,'v':0.02,'r':0.21,'i':0.45,'j':0.91,'h':1.39,'k':1.85}
        return hm(halo.s[ok], "v")+ABcorr['v'], hm(halo.s[ok], "b")+ABcorr['b'], hm(halo.s[ok], "k")+ABcorr['k'], hm(halo.s[ok], "u")+ABcorr['u'], hm(halo.s[ok], "j")+ABcorr['j'], hm(halo.s[ok], "i")+ABcorr['i']

    @classmethod
    def name(self):
        return "AB_V", "AB_B", "AB_K", "AB_U", "AB_J", "AB_I"

@pynbody.analysis.profile.Profile.profile_property
def magnitudes_encl(self,band='v'):
    mag = np.zeros(self.nbins)
    ltot = 0.0
    for i in range(self.nbins):
        if len(self.binind[i]) > 0:
            subs = self.sim[self.binind[i]]
            ltot += np.sum(10**(-0.4*subs[band+'_mag']))
        mag[i] = -2.5 * np.log10(ltot)

    return mag

class ABMagnitudes_encl(HaloProperties):
    @classmethod
    def name(self):
        return "AB_V_encl", "AB_B_encl", "AB_K_encl", "AB_U_encl", "AB_J_encl", "AB_I_encl", "AB_R_encl"

    def requires_property(self):
        return "SSC", "Rvir"

    def rstat(self, halo, maxrad, delta=0.1):
        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)
        pro = pynbody.analysis.profile.Profile(halo.s[pynbody.filt.HighPass("tform", 0)], type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
        return pro['magnitudes_encl,v'], pro['magnitudes_encl,b'], pro['magnitudes_encl,k'], pro['magnitudes_encl,u'], pro['magnitudes_encl,j'], pro['magnitudes_encl,i'], pro['magnitudes_encl,r']

    def calculate(self,halo,properties):
        com = properties['SSC']
        rad = properties['Rvir']
        halo["pos"] -= com
        halo.wrap()
        delta = properties.get('delta',0.1)

        V_encl, B_encl, K_encl, U_encl, J_encl, I_encl, R_encl = self.rstat(halo, rad, delta)
        halo["pos"] += com
        halo.wrap()

        ABcorr = {'u':0.79,'b':-0.09,'v':0.02,'r':0.21,'i':0.45,'j':0.91,'h':1.39,'k':1.85}

        return V_encl+ABcorr['v'], B_encl+ABcorr['b'], K_encl+ABcorr['k'], U_encl+ABcorr['u'], J_encl+ABcorr['j'], I_encl+ABcorr['i'], R_encl+ABcorr['r']

    def plot_x0(cls):
        return 0.0

    def plot_xdelta(cls):
        return 0.1

class HalfLight(HaloProperties):
    @classmethod
    def name(self):
        return "Rhalf_V", "Rhalf_B", "Rhalf_K", "Rhalf_U", "Rhalf_J", "Rhalf_I", "Rhalf_R"

    def requires_property(self):
        return ["SSC"]

    def calculate(self, halo, properties):
        com = properties['SSC']
        halo["pos"] -= com
        halo.wrap()

        rhalf = {'v':0.0, 'b':0.0, 'k':0.0, 'u':0.0, 'j':0.0, 'i':0.0, 'r':0.0}

        for key in rhalf.keys():
            rhalf[key] = pynbody.analysis.luminosity.half_light_r(halo.s[pynbody.filt.HighPass("tform", 0)], band=key)

        halo["pos"] += com
        halo.wrap()

        return rhalf['v'], rhalf['b'], rhalf['k'], rhalf['u'], rhalf['j'], rhalf['i'], rhalf['r']
