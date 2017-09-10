from . import HaloProperties
import numpy as np
import math
import pynbody
import scipy


class DPot2(HaloProperties):

    def calculate(self, halo, exist):
        import pynbody.analysis.gravity as grav

        phi1 = grav.potential(halo, exist['SSC'], unit="km^2 s^-2")
        phi2 = grav.potential(
            halo[pynbody.filt.Sphere(3.0, exist['SSC'])], exist['SSC'], unit="km^2 s^-2")

        return phi1, phi2

    @classmethod
    def name(self):
        return "dphi_pb", "dphi_pb_3kpc"

    def requires_property(self):
        return ["SSC"]


class DPotDM(HaloProperties):

    def calculate(self, halo, exist):
        import pynbody.analysis.gravity as grav
        halo = halo.dm
        phi1 = grav.potential(halo, exist['SSC'], unit="km^2 s^-2")
        phi2 = grav.potential(
            halo[pynbody.filt.Sphere(3.0, exist['SSC'])], exist['SSC'], unit="km^2 s^-2")

        return phi1, phi2

    @classmethod
    def name(self):
        return "dphi_dm", "dphi_dm_3kpc"

    def requires_property(self):
        return ["SSC"]


class SphericalPotential(HaloProperties):
    def requires_property(self):
        return ['dm_mass_profile']

    def calculate(self, halo, exist):
        mass = np.asarray(exist['dm_mass_profile'])

        max_x = len(mass)*0.1
        pot_dx=0.01

        r = np.arange(0.05,0.04+0.1*len(mass),0.1)
        rnew = np.arange(pot_dx,max_x,pot_dx)

        min_r_i = int(0.21/pot_dx)

        G = 4.30e-6 # km^2 s^-2 kpc^-1 Msol^-1

        mass_new = np.ones(len(rnew))

        mass_new[min_r_i:] = scipy.interp(rnew[min_r_i:],r,mass)
        mass_new[:min_r_i] = mass_new[min_r_i]*(rnew[:min_r_i]/rnew[min_r_i])**3
        # mass_new[min_r_i:]=0

        force = G*mass_new/(rnew**2)

        #force_coarse = G*np.asarray(mass)/(r**2)
        #force = np.ones(len(rnew))
        #force[min_r_i:] = scipy.interp(rnew[min_r_i:],r,force_coarse)

        ## fill in remainder of potential assuming constant density,
        ## so force goes as r
        #force[:min_r_i] = force[min_r_i]*(rnew[:min_r_i]/rnew[min_r_i])


        pot = np.cumsum(pot_dx*force)

        return pot[::10]

    @classmethod
    def name(self):
        return "dm_spherical_potential"

    @classmethod
    def requires_simdata(self):
        return False
