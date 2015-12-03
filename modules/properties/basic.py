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

class SSCComoving(HaloProperties):
    def name(self):
        return "SSC_comoving"

    def requires_simdata(self):
        return False

    def no_proxies(self):
        return True

    def calculate(self, halo, properties):
        return properties['SSC']*(1.+properties.timestep.redshift)

class MColdGas(HaloProperties):

    def calculate(self, halo, exist):
        coldgas = halo.gas[pynbody.filt.LowPass("temp", 2.e4)]["mass"].sum()
        higas = (halo.gas["mass"] * halo.gas["HI"]).sum()
        return coldgas, higas

    def name(self):
        return "MColdGas", "MHIGas"


class Contamination(HaloProperties):

    def calculate(self, halo, exist):
        n_heavy = (halo.dm['mass'] > halo.ancestor.dm['mass'].min()).sum()
        return float(n_heavy) / len(halo.dm)

    def name(self):
        return "contamination_fraction"


class HaloVvir(HaloProperties):

    def name(self):
        return "Vvir"

    def requires_simdata(self):
        return False

    def calculate(self, halo, properties):
        _G = 4.33227e-6
        return math.sqrt(_G * properties["Mvir"] / properties["Rvir"])


class HaloRmax(HaloProperties):

    def name(self):
        return "Rmax"

    def calculate(self, halo, properties):
        halo["pos"] -= properties["SSC"]
        rmx = pynbody.derived.r(halo).max()
        halo["pos"] += properties["SSC"]
        return rmx


class Softening(HaloProperties):

    def name(self):
        return "eps", "epsmin", "epsmax"

    def calculate(self, halo, existing_properties):
        return halo.mean_by_mass("eps"), halo["eps"].min(), halo["eps"].max()




class Metallicity(HaloProperties):
    # include

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

    def name(self):
        return "V", "B", "K", "U", "J", "I"


class ABMagnitudes(HaloProperties):

    def calculate(self, halo, existing_properties):
        hm = pynbody.analysis.luminosity.halo_mag
        ok, = np.where(halo.s['tform']>0)
        ABcorr = {'u':0.79,'b':-0.09,'v':0.02,'r':0.21,'i':0.45,'j':0.91,'h':1.39,'k':1.85}
        return hm(halo.s[ok], "v")+ABcorr['v'], hm(halo.s[ok], "b")+ABcorr['b'], hm(halo.s[ok], "k")+ABcorr['k'], hm(halo.s[ok], "u")+ABcorr['u'], hm(halo.s[ok], "j")+ABcorr['j'], hm(halo.s[ok], "i")+ABcorr['i']

    def name(self):
        return "AB_V", "AB_B", "AB_K", "AB_U", "AB_J", "AB_I"
