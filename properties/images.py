from . import HaloProperties
import numpy as np
import math
import pynbody


class Images(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "gas_image_sideon", "stellar_image_sideon", "gas_image_faceon", "stellar_image_faceon", "gas_image_original", "stellar_image_original"

    @classmethod
    def plot_extent(cls):
        return 15.0

    @classmethod
    def plot_xlabel(cls):
        return "x/kpc"

    @classmethod
    def plot_ylabel(cls):
        return "y/kpc"

    @classmethod
    def plot_clabel(cls):
        return r"M$_{\odot}$ kpc$^{-2}$"

    def render_projected(self, f, size):
        im = pynbody.plot.sph.image(f[pynbody.filt.BandPass(
            'z', -size / 2, size / 2)], 'rho', size, units="Msol kpc^-2", noplot=True)
        return im

    def render_gas(self, f, size):
        if len(f.gas)>0:
            return self.render_projected(f.gas, size)
        else:
            return None

    def render_stars(self, f, size):
        if len(f.st)>0:
            return pynbody.plot.stars.render(f.st[pynbody.filt.HighPass('tform',0) & pynbody.filt.BandPass('z', -size / 2, size / 2)],
                                         width=size, plot=False, ret_im=True, mag_range=(16,22))
        else:
            return None

    def requires_property(self):
        return ["SSC"]

    def calculate(self, halo, properties):
        import pynbody.plot
        size = 15.0
        f = halo.ancestor
        f['pos'] -= properties['SSC']

        g, s = self.render_gas(f, size), self.render_stars(f, size)
        try:
            tx = pynbody.analysis.angmom.sideon(halo, return_transform=True)
        except ValueError:
            tx = None
        if tx is not None:
            g_side, s_side = self.render_gas(f, size), self.render_stars(f, size)
            f.rotate_x(90)
            g_face, s_face = self.render_gas(f, size), self.render_stars(f, size)
            f.rotate_x(-90)
            tx.revert()
        else:
            g_side = None
            s_side = None
            g_face = None
            s_face = None
        #g, s = self.render_gas(f, size), self.render_stars(f, size)
        f['pos'] += properties['SSC']
        return g_side, s_side, g_face, s_face, g, s


class DmImages(Images):
    @classmethod
    def name(self):
        return "dm_image_z", "dm_image_x"

    @classmethod
    def plot_extent(cls):
        return 1000.0

    @classmethod
    def plot_xlabel(cls):
        return "x/kpc comoving"

    @classmethod
    def plot_ylabel(cls):
        return "y/kpc comoving"

    def calculate(self, halo, properties):
        import pynbody.plot
        f = halo.ancestor
        f['pos'] -= properties['SSC']
        im_z = self.render_projected(f.dm, self.plot_extent()*f.properties['a'])
        tx = f.rotate_y(90)
        im_x = self.render_projected(f.dm, self.plot_extent()*f.properties['a'])
        tx.revert()
        f['pos'] += properties['SSC']
        return im_z, im_x





class InflowOutflowImg(HaloProperties):

    @classmethod
    def name(self):
        return "virial_rho", "virial_vr", "virial_rho_Z"

    def requires_property(self):
        return "SSC", "Rvir"

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
