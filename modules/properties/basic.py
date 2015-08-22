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


class StarForm(HaloProperties):

    """Properties relating to star formation"""

    # include

    def name(self):
        return "SFR", "SFR_1kpc", "SFR_250Myr", "SFR_25Myr"

    def calcForHalo(self, halo, period=2.5e7, hard_period=False):
        sts = halo.star
        if len(sts) == 0:
            return 0.

        try:
            tunit = sts['tform'].units
        except KeyError:
            tunit = sts['age'].units

        conv = tunit.ratio("yr")
        # getUnits().convertTo(siman.Unit("Msol"))
        convmass = sts["mass"].units.ratio("Msol")
        now = halo.properties['time'].in_units(tunit)

        if len(sts) > 2:
            mass = sts["mass"]

            def get_period():
                try:
                    tf = sts["tform"]
                    index_period = np.nonzero(tf > now - period / conv)[0]
                except KeyError:
                    tf = sts['age']
                    index_period = np.nonzero(tf < period / conv)[0]
                return index_period

            index_period = get_period()

            while len(index_period) < 2 and period < 1.e9 and not hard_period:
                period *= 2.
                index_period = get_period()

            mass_formed = mass[index_period].sum() * convmass
            return mass_formed / period
        else:
            return 0.

    def calculate(self, halo, existing_properties):
        return self.calcForHalo(halo), self.calcForHalo(halo[pynbody.filt.Sphere("1 kpc", existing_properties["SSC"])]), \
            self.calcForHalo(halo, 2.5e8),\
            self.calcForHalo(halo, hard_period=True)

    def spherical_region(self):
        return True


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
        return hm(halo, "V"), hm(halo, "B"), hm(halo, "K")

    def name(self):
        return "V", "B", "K"
