from . import HaloProperties
import numpy as np
import math
import pynbody


class REnergy(HaloProperties):

    @classmethod
    def name(self):
        return "ramses_pe", "ramses_ke", "ramses_te"

    def spherical_region(self):
        return True

    def calculate(self, halo, existing_properties):
        f = halo.ancestor
        """
        f['pos']-=existing_properties['SSC']
        rmax = existing_properties['Rvir']*1.0
        nx = 200
        try:
            halo = f[pynbody.filt.Sphere(rmax)]

            # correct potential so phi=0 at infinity
            f.gas['phi']-=f.gas['phi'].max()

            rho_im = pynbody.sph.to_3d_grid(halo.dm, nx=nx, x2=rmax,
                                            approximate_fast=True, threaded=8, denoise=True)
            rho_im[np.isnan(rho_im)]=0
            self.rho_im = rho_im
            phi_im = pynbody.sph.to_3d_grid(f.gas, nx=nx, x2=rmax, qty='phi',
                                            threaded=8, denoise=True)
            phi_im[np.isnan(phi_im)]=0
            self.phi_im0 = phi_im

            # OR....
            dx = 2*rmax/nx

            acx = pynbody.sph.render_image(f.gas, nx=nx,x2=rmax,qty='accg_x', denoise=True)
            acx[np.isnan(acx)]=0
            acy = pynbody.sph.render_image(f.gas, nx=nx,x2=rmax,qty='accg_y', denoise=True)
            acy[np.isnan(acy)]=0
            phi0_strip=acx[:,nx/2].cumsum()*dx
            phi0_plane=acy.cumsum(axis=1)*dx
            phi0_plane-=phi0_plane[:,nx/2]
            phi0_plane+=phi0_strip
            
            phi_im = pynbody.sph.to_3d_grid(f.gas, nx=nx, x2=rmax,qty='accg_z',
                                            approximate_fast=True, threaded=8, denoise=True)
            
            phi_im[np.isnan(phi_im)]=0            
            phi_im = -phi_im.cumsum(axis=2)
            phi_im*=2*rmax/nx
            self.pldiff = phi_im[:,:,nx/2]-phi0_plane
            phi_im-=phi_im[:,:,nx/2]
            phi_im+=phi0_plane

            phi_im.units = phi_im.units*pynbody.units.kpc
            phi_im.convert_units('km^2 s^-2')
            self.phi_im = phi_im
            
        finally:
            f['pos']+=existing_properties['SSC']
            

        pe_den = 0.5*(rho_im*phi_im).sum().in_units("erg kpc^-3")
        volume = (2*rmax/nx)**3
        pe = float(pe_den)*volume"""

        # new
        pe = 0.5 * (f.dm['phi'] * f.dm['mass']).in_units('erg').sum()
        # sometimes we end up with zeros
        # in the potential output. Here is a kludgy part fix;
        # simply pretend the particles with non-zero phi unbiasedly
        # sample the halo and rescale up to the full mass...
        pe *= f.dm['mass'].sum() / (f.dm['mass'][f.dm['phi'] != 0]).sum()
        if False:
            # old
            sub = halo.dm[::500]
            phi, acc = pynbody.gravity.calc.direct(
                halo.dm, np.asarray(sub['pos']), 0.1)
            pe = (0.5 * phi * sub['mass']).sum() * \
                halo.dm['mass'].sum() / sub['mass'].sum()
            pe = pe.in_units('erg')
        ke = (halo.dm['mass'] * halo.dm['ke']).sum().in_units("erg")
        return pe, ke, pe + ke


class HaloSelfEnergy(HaloProperties):

    @classmethod
    def name(self):
        return "selfenergy", "dm_selfenergy"

    def energy(self, halo):
        import copy
        phi = copy.copy(halo["phi"])
        phi -= phi.max()
        pe = (halo["mass"] * phi).in_units("Msol km^2 s^-2").sum()
        ke = (halo["mass"] * (halo["vel"] ** 2).sum(axis=1)).sum()
        return pe + ke

    def calculate(self, halo, existing_properties):
        return self.energy(halo), self.energy(halo.dm)


class GasProfilePhi(HaloProperties):

    @classmethod
    def name(self):
        return ["gas_profile_phi"]

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return ["gas_mass_profile"]

    def calculate(self, halo, existing_properties):
        mass = existing_properties['gas_mass_profile']
        r = np.arange(0.05, len(mass) * 0.1 + 0.04, 0.1)
        G = 4.302e-6  # km^2 s^-2 kpc Msol^-1
        phi = G * (0.1 * mass / r ** 2).cumsum()
        return phi


class ProfilePhi(HaloProperties):

    @classmethod
    def name(self):
        return "profile_phi"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return ["tot_mass_profile"]

    def calculate(self, halo, existing_properties):
        mass = existing_properties['tot_mass_profile']
        r = np.arange(0.05, len(mass) * 0.1 + 0.04, 0.1)
        G = 4.302e-6  # km^2 s^-2 kpc Msol^-1
        phi = G * (0.1 * mass / r ** 2).cumsum()
        return phi


class ProfileSelfEnergy(HaloProperties):

    @classmethod
    def name(self):
        return "dm_profile_pe", "dm_profile_selfenergy", "dm_profile_Ir2"

    @classmethod
    def requires_simdata(self):
        return False

    def calculate(self, halo, existing_properties):
        mass = existing_properties["dm_mass_profile"]

        r = np.arange(0.05, len(mass) * 0.1 + 0.04, 0.1)
        G = 8.56e37  # ergs kpc/Msol^2
        PE = -G * (0.1 * mass * mass / r ** 2).sum() / \
            2 - G * mass[-1] ** 2 / (2 * r[-1])
        MM = 2 * (mass * r * 0.1).sum()
        dM2 = existing_properties['dm_mass_profile'][-1] * r[-1] ** 2
        return PE, PE / 2, dM2 - MM
