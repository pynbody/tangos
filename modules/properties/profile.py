from . import HaloProperties, instantiate_class, providing_class, LiveHaloProperties, LiveHaloPropertiesInheritingMetaProperties
import numpy as np
import math
import pynbody
import scipy.optimize

class CoreSize(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "coresize"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return "dm_density_profile",

    def calculate(self, halo, properties):
        import core_fit
        # return core_fit.get_bestfit(properties)[2]
        return core_fit.get_core_radius(properties, plot=False)


class VcircSpherical(HaloProperties):
    @classmethod
    def plot_x0(cls):
        return 0.05

    @classmethod
    def plot_ylog(cls):
        return False

    @classmethod
    def plot_xdelta(cls):
        return 0.1

    @classmethod
    def plot_xlabel(cls):
        return "r/kpc"

    @classmethod
    def plot_ylabel(cls):
        return r"$\sqrt{GM/r}$"

    @classmethod
    def name(self):
        return "vcirc_spherical"

    def requires_property(self):
        return ["dm_density_profile", "tot_mass_profile"]

    @classmethod
    def requires_simdata(self):
        return False

    def calculate(self, halo, props) :
        M = props['tot_mass_profile']
        r = np.arange(0.05,len(M)*0.1,0.1)
        G = 4.3012e-6
        v = np.sqrt(G*M/r)
        return v


class VmaxDM(HaloProperties):

    @classmethod
    def name(self):
        return "vmax_dm_local", "rmax_dm_local", "vmax_dm_global"

    def requires_property(self):
        return ["dm_mass_profile"]

    @classmethod
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

    @classmethod
    def name(self):
        return "dm_alpha_500pc", "dm_alpha_1kpc"

    def requires_property(self):
        return ["dm_density_profile"]

    @classmethod
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

    @classmethod
    def name(self):
        return "dm_density_profile", "dm_mass_profile", "tot_density_profile", "tot_mass_profile", "gas_density_profile", "gas_mass_profile", "star_density_profile", "star_mass_profile"

    def spherical_region(self):
        return True

    @classmethod
    def plot_x0(cls):
        return 0.05

    @classmethod
    def plot_xdelta(cls):
        return 0.1

    @classmethod
    def plot_xlabel(cls):
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

    def requires_property(self):
        return ["SSC", "Rvir"]



class StellarProfileFaceOn(HaloProperties):
    @classmethod
    def name(self):
        return "v_surface_brightness", "b_surface_brightness", "i_surface_brightness"

    @classmethod
    def plot_x0(cls):
        return 0.05

    @classmethod
    def plot_xdelta(cls):
        return 0.1

    @classmethod
    def plot_xlabel(cls):
        return "R/kpc"

    @classmethod
    def plot_ylog(cls):
        return False

    @classmethod
    def plot_xlog(cls):
        return False

    @staticmethod
    def plot_ylabel():
        return "v mags/arcsec$^2$", "b mags/arcsec$^2$", "i mags/arcsec$^2$"

    def calculate(self, halo, existing_properties):
        with pynbody.analysis.angmom.faceon(halo):
            ps = pynbody.analysis.profile.Profile(halo.s, type='lin', ndim=2, min=0, max=20, nbins=200)
            vals = [ps['sb,'+x] for x in 'v','b','i']
        return vals

def sersic_surface_brightness(r, s0, r0, n) :
    # 2.5 / ln 10 = 1.08573620
    return s0 + 1.08573620 * (r/r0)**(1./n)

def fit_sersic(r, surface_brightness, return_cov=False):
    s0_guess = np.mean(surface_brightness[:3])
    s0_range = [s0_guess-2, s0_guess+2]
    r0_range = [0.1,r[-1]]
    n_range = [0.1, 6.0]

    r0_guess = r[len(r)/2]
    n_guess = 1.0

    sigma = 10**(0.6*(surface_brightness-20))/r
    sigma = None

    popt, pcov = scipy.optimize.curve_fit(sersic_surface_brightness,r,surface_brightness,
                                          bounds=np.array((s0_range, r0_range, n_range)).T,
                                          sigma=sigma,
                                          p0=(s0_guess, r0_guess, n_guess))

    if return_cov:
        return popt, pcov
    else:
        return popt

class GenericPercentile(HaloProperties):
    def __init__(self, simulation, ratio, name):
        super(GenericPercentile, self).__init__(simulation)
        self._name_info = name

    @classmethod
    def name(self):
        return "percentile"

    @classmethod
    def requires_simdata(self):
        return False

    def live_calculate(self, halo, ratio, ar):
        x0 = self._name_info.plot_x0()
        delta_x = self._name_info.plot_xdelta()
        ar/=ar[-1]
        index = np.where(ar>ratio)[0][0]
        return x0+index*delta_x

class AtPosition(LiveHaloProperties):
    def __init__(self, simulation, position, array):
        super(AtPosition, self).__init__(simulation)
        self._array_info = array

    @classmethod
    def name(cls):
        return "at"

    def live_calculate(self, halo, pos, ar):
        x0 = self._array_info.plot_x0()
        delta_x = self._array_info.plot_xdelta()

        # linear interpolation
        i0 = int((pos-x0)/delta_x)
        i1 = i0+1

        i0_loc = float(i0)*delta_x+x0
        i1_weight = (pos-i0_loc)/delta_x
        i0_weight = 1.0-i1_weight

        if i1>=len(ar) or i0<0:
            return None
        else:
            return ar[i0]*i0_weight + ar[i1]*i1_weight

class MaxMinProperty(LiveHaloProperties):
    def __init__(self, simulation, array):
        super(MaxMinProperty, self).__init__(simulation)
        self._array_info = array

    @classmethod
    def name(cls):
        return "max", "min", "posmax", "posmin"

    def live_calculate(self, halo, array):
        max_, min_ = np.max(array), np.min(array)
        amax, amin = np.argmax(array), np.argmin(array)
        index_to_r = lambda index: index*self._array_info.plot_xdelta()+self._array_info.plot_x0()
        return float(max_), float(min_), index_to_r(amax), index_to_r(amin)


class StellarProfileDiagnosis(LiveHaloProperties):
    def __init__(self, simulation, band):
        super(StellarProfileDiagnosis, self).__init__(simulation)
        self.band = band

    @classmethod
    def name(self):
        return "half_light","sersic_m0", "sersic_n", "sersic_r0"

    @classmethod
    def requires_simdata(self):
        return False

    def requires_property(self):
        return self.band+"_surface_brightness",

    def calculate(self, halo, existing_properties):
        r0 = 0.05
        delta_r = 0.1
        surface_brightness = existing_properties[self.band+"_surface_brightness"]
        flux_density = 10**(surface_brightness/-2.5)
        flux_density[flux_density!=flux_density]=0
        nbins = len(surface_brightness)
        r = np.arange(r0,r0+delta_r*nbins,delta_r)
        cumu_flux_density = (r * flux_density).cumsum()
        cumu_flux_density/=cumu_flux_density[-1]

        half_light_i = np.where(cumu_flux_density>0.5)[0][0]
        half_light = r0+delta_r * half_light_i

        r_fit = r[4:half_light_i*5]
        sb_fit = surface_brightness[4:half_light_i*5]

        mask_not_nan = sb_fit==sb_fit
        r_fit = r_fit[mask_not_nan]
        sb_fit = sb_fit[mask_not_nan]
        m0, n, r0 = fit_sersic(r_fit, sb_fit)
        return half_light, m0, n, r0
