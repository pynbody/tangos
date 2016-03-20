from . import HaloProperties
import numpy as np
import math
import pynbody


class OutflowEnergy(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "outflow_energy_1", "SFR_250Myr_energy"

    @classmethod
    def requires_simdata(self):
        return False

    def calculate(self, halo, properties):
        # ergs per year
        return (1.98e43 * properties["outflow_1"] * properties["vel_out_1"] ** 2) / 2,  \
            1.0e49 * properties["SFR_250Myr"]


class TestInflowOutflow(HaloProperties):

    @classmethod
    def name(self):
        return "test_inflow", "test_vel_inflow", "test_outflow", "test_vel_outflow"

    def calc_inflow_outflow(self, f, radius):
        # assumes already centred, makes inflow/outflow rates
        import pynbody.sph

        vz = pynbody.sph.render_image(
            f.gas, 'vz', radius, 600, out_units="km s^-1")
        rho = pynbody.sph.render_image(
            f.gas, 'rho', radius, 600, out_units="Msol kpc^-3")

        dA = (radius * 2.0 / 600) ** 2 * pynbody.units.Unit("kpc") ** 2
        massflow = (rho * vz * dA)
        massflow.convert_units("Msol yr^-1")
        outflow = massflow[np.where(massflow > 0)].sum()
        inflow = -massflow[np.where(massflow < 0)].sum()

        velin = (massflow * vz)[np.where(massflow < 0)].sum() / inflow
        velout = (massflow * vz)[np.where(massflow > 0)].sum() / outflow

        return inflow, velin, outflow, velout

    def preloop(self, f, filename, pa):
        self.f = f

    def calculate(self, halo, properties):
        cen, radius = properties["SSC"], properties["Rvir"]
        f = self.f

        self.f["pos"] -= cen
        vcen = self.f.dm[pynbody.filt.Sphere(1.0)].mean_by_mass("vel")
        if vcen[0] != vcen[0]:
            print "DISASTER! Can't velocity centre"
            i, vi, o, vo = 0, 0, 0, 0
        else:
            self.f['vel'] -= vcen
            f['z'] -= radius
            f.gas['smooth'] = (f.gas['mass'] / f.gas['rho']) ** (1, 3)
            i, vi, o, vo = self.calc_inflow_outflow(self.f.gas, radius)
            f['z'] += 2 * radius
            i2, vi2, o2, vo2 = self.calc_inflow_outflow(self.f.gas, radius)
            i, vi, o, vo = [
                (a + b) / 2 for a, b in zip((i, vi, o, vo), (i2, vi2, o2, vo2))]
            f['z'] -= radius
            self.f["vel"] += vcen

        self.f["pos"] += cen
        return i, vi, o, vo


class FlowProfile(HaloProperties):
    _xmax = 300.0
    _threshold_vel = 20.0

    @classmethod
    def name(cls):
        return "inflow_Mdot", "outflow_Mdot", "inflow_vel", "outflow_vel", "inflow_temp", "outflow_temp"

    @classmethod
    def plot_x0(cls):
        return 2.5

    @classmethod
    def plot_xdelta(cls):
        return 5.0

    def generate_cells(self, halo):
        nx = 2*self._xmax/self.plot_xdelta()
        grid_vr = pynbody.sph.to_3d_grid(halo.ancestor.gas, 'vr', nx, x2=self._xmax,denoise=True)
        grid_rho = pynbody.sph.to_3d_grid(halo.ancestor.gas, 'rho', nx, x2=self._xmax,denoise=True)
        grid_r = pynbody.sph.to_3d_grid(halo.ancestor.gas, 'r', nx, x2=self._xmax,denoise=True)
        grid_temp = pynbody.sph.to_3d_grid(halo.ancestor.gas, 'temp', nx, x2=self._xmax,denoise=True)
        Mdot = ((grid_rho*grid_vr*4.0*np.pi*grid_r**2).in_units("Msol yr^-1")).flatten()
        vr = grid_vr.flatten()
        r = grid_r.flatten()
        return r,vr,Mdot,grid_temp.flatten()

    def cells_to_r_bins(self, r, vr, Mdot, T, vr_cut):
        ar_length = int(self._xmax/self.plot_xdelta())
        if vr_cut>0:
            mask = vr>vr_cut
        else:
            mask = vr<vr_cut

        r_index = np.array((r-self.plot_x0())/self.plot_xdelta(),dtype=int)

        bincount = np.bincount(r_index, minlength=ar_length)[:ar_length]
        Mdot_tot = np.bincount(r_index[mask],Mdot[mask],minlength=ar_length)[:ar_length]
        vr_tot = np.bincount(r_index[mask],vr[mask]*Mdot[mask],minlength=ar_length)[:ar_length]
        temp_tot = np.bincount(r_index[mask],T[mask]*Mdot[mask],minlength=ar_length)[:ar_length]

        return Mdot_tot/bincount, vr_tot/Mdot_tot, temp_tot/Mdot_tot


    def calculate(self, halo, properties):
        with pynbody.analysis.halo.center(halo.gas):
            r,vr,Mdot,T = self.generate_cells(halo.gas)
            inflow_Mdot, inflow_vel, inflow_temp = self.cells_to_r_bins(r,vr,Mdot,T,-self._threshold_vel)
            outflow_Mdot, outflow_vel, outflow_temp = self.cells_to_r_bins(r,vr,Mdot,T,self._threshold_vel)

        return -inflow_Mdot, outflow_Mdot, -inflow_vel, outflow_vel,  inflow_temp, outflow_temp



class InflowOutflow(HaloProperties):

    """Inflow and outflow rates at various multiples of the virial radius, in Msol yr^-1.
    AP 25/01/2011."""

    # include

    @classmethod
    def name(self):
        return "inflow_.5", "outflow_.5", "vel_in_.5", "vel_out_.5", "inflow_1", "outflow_1", "vel_in_1", "vel_out_1", "inflow_2", "outflow_2", "vel_in_2", "vel_out_2"

    def calc_inflow_outflow(self, f, radius, cen=None):
        import pynbody.sph
        import healpy as hp
        if cen is not None:
            f["pos"] -= cen

        rho = pynbody.sph.render_spherical_image(
            f.gas, "rho", distance=radius, nside=32)
        vr = pynbody.sph.render_spherical_image(
            f.gas, "vr", distance=radius, nside=32)
        massflow = (rho * vr)

        A = (4 * math.pi * (radius * pynbody.units.Unit("kpc")) ** 2)
        massflow.units *= A / len(massflow)
        massflow.convert_units("Msol yr^-1")
        #massflow, mono, dipole = hp.remove_dipole(massflow, fitval=True)
        # massflow+=mono # add back on removed monopole
        outflow = massflow[np.where(massflow > 0)].sum()
        inflow = -massflow[np.where(massflow < 0)].sum()
        velin = (massflow * vr)[np.where(massflow < 0)].sum() / inflow
        velout = (massflow * vr)[np.where(massflow > 0)].sum() / outflow

        print vr.max(), -vr.min(), velin, velout
        if cen is not None:
            f["pos"] += cen

        return inflow, outflow, velin, velout

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
        vcen = self.f.dm[pynbody.filt.Sphere(5.0)].mean_by_mass("vel")
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
        import time
        print "Doing inflow/outflow..."
        s = time.time()

        self.pre_offset(halo, properties)

        print self.f['vel'][0], self.f['pos'][0], self.f['vr'][0]

        i1, o1, vi1, vo1 = self.calc_inflow_outflow(
            self.f.gas, properties["Rvir"] * 0.5)
        i2, o2, vi2, vo2 = self.calc_inflow_outflow(
            self.f.gas, properties["Rvir"])
        i3, o3, vi3, vo3 = self.calc_inflow_outflow(
            self.f.gas, properties["Rvir"] * 2.0)

        self.post_offset()

        print "x-check", self.f["pos"][0]
        print "Done in %.1f s" % (time.time() - s)
        print "At virial radius inflow, outflow = ", i2, o2, vi2, vo2
        return i1, o1, vi1, vo1, i2, o2, vi2, vo2, i3, o3, vi3, vo3


class InflowOutflow60Kpc(InflowOutflow):
    # include

    @classmethod
    def name(self):
        return "inflow_15kpc", "outflow_15kpc", "vel_in_15kpc", "vel_out_15kpc"

    def calculate(self, halo, properties):
        self.pre_offset(halo, properties)
        ret = self.calc_inflow_outflow(self.f.gas, 15.)
        self.post_offset()
        return ret
