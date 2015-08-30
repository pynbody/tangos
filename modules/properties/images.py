from . import HaloProperties
import numpy as np
import math
import pynbody


class Images(HaloProperties):
    # include

    def name(self):
        return "gas_image_sideon", "stellar_image_sideon", "gas_image_faceon", "stellar_image_faceon", "gas_image_original", "stellar_image_original"

    @staticmethod
    def plot_extent():
        return 20.0

    @staticmethod
    def plot_xlabel():
        return "x/kpc"

    @staticmethod
    def plot_ylabel():
        return "y/kpc"

    @staticmethod
    def plot_clabel():
        return r"M$_{\odot}$ kpc$^{-2}$"

    def render_gas(self, f, size):
        im = pynbody.plot.sph.image(f.gas[pynbody.filt.BandPass(
            'z', -size / 2, size / 2)], 'rho', size, units="Msol kpc^-2", noplot=True)
        return im

    def render_stars(self, f, size):
        return pynbody.plot.stars.render(f[pynbody.filt.BandPass('z', -size / 2, size / 2)], width=size, plot=False, ret_im=True)

    def calculate(self, halo, properties):
        size = 15.0
        f = halo.ancestor
        f['pos'] -= properties['SSC']
        tx = pynbody.analysis.angmom.sideon(halo, return_transform=True)
        g_side, s_side = self.render_gas(f, size), self.render_stars(f, size)
        f.rotate_x(90)
        g_face, s_face = self.render_gas(f, size), self.render_stars(f, size)
        f.rotate_x(-90)
        tx.revert()
        g, s = self.render_gas(f, size), self.render_stars(f, size)
        return g_side, s_side, g_face, s_face, g, s


class InflowOutflowImg(HaloProperties):

    def name(self):
        return "virial_rho", "virial_vr", "virial_rho_Z"

    def calc_imgs(self, f, radius):
        import pynbody.sph
        import healpy as hp
        f.gas['rho_Z'] = f.gas['rho'] * f.gas['metals']
        rho = pynbody.sph.render_spherical_image(
            f.gas, "rho", distance=radius, nside=32)
        vr = pynbody.sph.render_spherical_image(
            f.gas, "vr", distance=radius, nside=32)
        metal = pynbody.sph.render_spherical_image(
            f.gas, "rho_Z", distance=radius, nside=32)
        return rho, vr, metal

    def preloop(self, f, filename, pa):
        self.f = f

    def pre_offset(self, halo, properties):
        cen = properties["SSC"]

        print "p-min is at ", pynbody.analysis.halo.potential_minimum(halo.dm)

        print "x-check (check below)", self.f["pos"][0]
        print "offsetting to", cen
        self.f["pos"] -= cen
        print "p-min is now at ", pynbody.analysis.halo.potential_minimum(halo.dm)
        print "Rvir is ", properties["Rvir"]
        vcen = self.f.dm[pynbody.filt.Sphere(1.0)].mean_by_mass("vel")
        if vcen[0] != vcen[0]:
            raise RuntimeError, "Can't velocity centre"
        print "vel cen is ", vcen
        self.f["vel"] -= vcen
        self.vcen = vcen
        self.cen = cen

    def post_offset(self):
        self.f["vel"] += self.vcen
        self.f["pos"] += self.cen

    def calculate(self, halo, properties):

        self.pre_offset(halo, properties)
        rvals = self.calc_imgs(self.f.gas, properties["Rvir"])
        self.post_offset()

        return rvals