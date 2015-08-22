from . import HaloProperties
import numpy as np
import math
import pynbody


class CoreSize(HaloProperties):
    # include

    def name(self):
        return "coresize"

    def requires_simdata(self):
        return False

    def calculate(self, halo, properties):
        import core_fit
        # return core_fit.get_bestfit(properties)[2]
        return core_fit.get_core_radius(properties, plot=False)


class VcircSpherical(HaloProperties):
    @staticmethod
    def plot_x0():
        return 0.05

    @staticmethod
    def plot_ylog():
        return False

    @staticmethod
    def plot_xdelta():
        return 0.1

    @staticmethod
    def plot_xlabel():
        return "r/kpc"

    @staticmethod
    def plot_ylabel():
        return r"$\sqrt{GM/r}$"

    def name(self):
        return "vcirc_spherical"

    def requires_property(self):
        return ["dm_density_profile"]

    def requires_simdata(self):
        return False

    def calculate(self, halo, props) :
        M = props['tot_mass_profile']
        r = np.arange(0.05,len(M)*0.1,0.1)
        G = 4.3012e-6
        v = np.sqrt(G*M/r)
        return v


class VmaxDM(HaloProperties):

    def name(self):
        return "vmax_dm_local", "rmax_dm_local", "vmax_dm_global"

    def requires_property(self):
        return ["dm_density_profile"]

    def requires_simdata(self):
        return False

    def calculate(self, halo, props) :
        M = props['dm_mass_profile']
        r = np.arange(0.05,len(M)*0.1,0.1)
        G = 4.3012e-6
        v = np.sqrt(G*M/r)
        glob_max = v.max()
        try:
            imax =  np.where(np.diff(v)[5:]<0)[0][0]+5
            loc_max = v[imax]
        except IndexError:
            imax = v.argmax()
            loc_max = glob_max
        return loc_max, r[imax], glob_max


class HaloDensitySlope(HaloProperties):
    # include

    def name(self):
        return "dm_alpha_500pc", "dm_alpha_1kpc"

    def requires_property(self):
        return ["dm_density_profile"]

    def requires_simdata(self):
        return False

    def calculate(self, halo, properties):
        # M_a = properties["dm_mass_profile"]
        rho_a = properties["dm_density_profile"]

        if all(rho_a[3:12] > 0):
            log_pos = np.log10(np.arange(3.5, 7.5, 1.0))
            log_rho = np.log10(rho_a[3:7])
            alpha, x = np.polyfit(log_pos, log_rho, 1)

            log_pos = np.log10(np.arange(8.5, 12.5, 1.0))
            log_rho = np.log10(rho_a[8:12])
            alpha2, x = np.polyfit(log_pos, log_rho, 1)

            # alpha = (np.log10(rho_a[3])-np.log10(rho_a[2]))/math.log10(3.5/2.5)
            # alpha = (np.log10(M_a[3])-np.log10(M_a[2]))/math.log10(3.5/2.5) - 2
            return alpha, alpha2
        else:
            return np.NaN, np.NaN


class HaloDensityProfile(HaloProperties):
    # include

    def name(self):
        return "dm_density_profile", "dm_mass_profile", "tot_density_profile", "tot_mass_profile", "gas_density_profile", "gas_mass_profile", "star_density_profile", "star_mass_profile"

    def spherical_region(self):
        return True

    @staticmethod
    def plot_x0():
        return 0.05

    @staticmethod
    def plot_xdelta():
        return 0.1

    @staticmethod
    def plot_xlabel():
        return "r/kpc"

    @staticmethod
    def plot_ylabel():
        return r"$\rho/M_{\odot}\,kpc^{-3}$", r"$M/M_{\odot}$", r"$\rho/M_{\odot}\,kpc^{-3}$", r"$M/M_{\odot}$", r"$\rho/M_{\odot}\,kpc^{-3}$", r"$M/M_{\odot}$", r"$\rho/M_{\odot}\,kpc^{-3}$", r"$M/M_{\odot}$"

    def rstat(self, halo, maxrad, cen, delta=0.1):
        mass_a = []
        rho_a = []

        mass_x = 0

        V_x = 0
        halo['pos'] -= cen
        halo.wrap()

        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)

        pro = pynbody.analysis.profile.Profile(halo, type='lin', ndim=3,
                                               min=0, max=maxrad, nbins=nbins)

        rho_a = pro['density']
        mass_a = pro['mass_enc']

        halo['pos'] += cen
        halo.wrap()
        rho_a = np.array(rho_a)
        mass_a = np.array(mass_a)

        return rho_a, mass_a

    def calculate(self, halo, existing_properties):

        halo.dm['mass']
        try:
            halo.gas['mass']
            halo.star['mass']
        except:
            pass
        delta = existing_properties.get('delta',0.1)
        self.mark_timer('dm')
        dm_a, dm_b = self.rstat(
            halo.dm, existing_properties["Rvir"], existing_properties["SSC"],delta)
        self.mark_timer('tot')
        tot_a, tot_b = self.rstat(
            halo, existing_properties["Rvir"], existing_properties["SSC"],delta)
        self.mark_timer('gas')
        gas_a, gas_b = self.rstat(
            halo.gas, existing_properties["Rvir"], existing_properties["SSC"],delta)
        self.mark_timer('star')
        star_a, star_b = self.rstat(
            halo.star, existing_properties["Rvir"], existing_properties["SSC"],delta)
        return dm_a, dm_b, tot_a, tot_b, gas_a, gas_b, star_a, star_b
