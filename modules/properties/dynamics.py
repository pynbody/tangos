from . import HaloProperties
import numpy as np
import math
import pynbody
try:
    import dyn_profile as dp
except ImportError:
    dp = None



class AngMom(HaloProperties):

    def spherical_region(self):
        return True

    def requires_property(self):
        return ["SSC"]

    def calculate(self, halo, exist):
        f = halo
        f['pos'] -= exist['SSC']

        velmean = f.dm[pynbody.filt.Sphere('1 kpc')]['vel'].mean(axis=0)
        print "velmean=", velmean
        f['vel'] -= velmean

        vec = pynbody.analysis.angmom.ang_mom_vec(
            f.star[pynbody.filt.Sphere('2 kpc')])
        print vec
        assert np.linalg.norm(vec) > 0
        vec /= np.linalg.norm(vec)
        print vec
        vecdm = pynbody.analysis.angmom.ang_mom_vec(
            f.dm[pynbody.filt.Sphere('2 kpc') & pynbody.filt.HighPass('mass', 0)])
        print vecdm
        vecdm /= np.linalg.norm(vecdm)

        vecgas = pynbody.analysis.angmom.ang_mom_vec(
            f.gas[pynbody.filt.Sphere('2 kpc')])
        vecgas /= np.linalg.norm(vecgas)

        f['vel'] += velmean
        f['pos'] += exist['SSC']

        return vecdm, vec, vecgas

    @classmethod
    def name(self):
        return "angmom_dm_2kpc", "angmom_star_2kpc", "angmom_gas_2kpc"


class MeanJr(HaloProperties):

    def spherical_region(self):
        return False

    @classmethod
    def name(self):
        return "Jr_dm", "Jr_dm_std"

    def requires_property(self):
        return "SSC"

    def calculate(self, f, exist):
        ptcl = f
        f = f.ancestor
        f['pos'] -= exist['SSC']
        velmean = f.ancestor.dm[
            pynbody.filt.Sphere('1 kpc')]['vel'].mean(axis=0)
        f.dm['vel'] -= velmean

        import dyn_profile
        # , mass_profile=exist['tot_mass_profile'])
        PM = dyn_profile.DynamicalProfile(
            f, ptcls=ptcl, eps='0.1 kpc', max_r='200 kpc', centre=False, vacuum=True, subsample=10)
        Jr = PM.get_Jr_array(threads=4)

        f['pos'] += exist['SSC']
        f.dm['vel'] += velmean

        Jr = Jr[np.where(Jr < np.inf)]

        return Jr.mean(), Jr.std()


class MeanESpher(HaloProperties):

    def spherical_region(self):
        return False

    @classmethod
    def name(self):
        return "E_spherical_dm", "E_spherical_dm_std"

    def requires_property(self):
        return "SSC"

    def calculate(self, f, exist):
        ptcl = f
        f = f.ancestor
        f['pos'] -= exist['SSC']
        velmean = f.ancestor.dm[
            pynbody.filt.Sphere('1 kpc')]['vel'].mean(axis=0)
        f.dm['vel'] -= velmean

        import dyn_profile
        # , mass_profile=exist['tot_mass_profile'])
        PM = dyn_profile.DynamicalProfile(
            f, ptcls=ptcl, eps='0.1 kpc', max_r='200 kpc', centre=False, vacuum=True, subsample=3)

        """
        f['pos'] += exist['SSC']
        f.dm['vel'] += velmean
        """
        return PM.ptcl_E.mean(), PM.ptcl_E.std()


class MeanR(HaloProperties):

    def spherical_region(self):
        return False

    @classmethod
    def name(self):
        return "r_dm"

    def requires_property(self):
        return "SSC"

    def calculate(self, f, exist):
        f['pos'] -= exist['SSC']
        rm = f.dm['r'].mean()
        f['pos'] += exist['SSC']
        return rm


class AngMomVecVsScalar(HaloProperties):

    def spherical_region(self):
        return False

    @classmethod
    def name(self):
        return "angmom_dm_500pc_scalar", "angmom_dm_500pc_vector", "angmom_dm_scalar", "angmom_dm_vector"

    def getvals(self, f_cen):
        jvec = np.asarray(f_cen['j'].mean(axis=0))
        jscal = np.sqrt(f_cen['j2']).mean()
        return jscal, jvec

    def requires_property(self):
        return "SSC"

    def calculate(self, f, exist):
        import scipy
        f_marked = f
        f = f.ancestor.dm
        f['pos'] -= exist['SSC']
        velmean = f[pynbody.filt.Sphere('1 kpc')]['vel'].mean(axis=0)
        f['vel'] -= velmean

        #mp = exist['tot_mass_profile']
        #massfn = lambda r : scipy.interp(r,np.linspace(0.05,0.1*(len(mp)-1)+0.05,len(mp)),mp)

        #r0 = pynbody.util.bisect(0.,10.,lambda r: massfn(r)-3e8, epsilon=0, eta=3e6, verbose=True)

        f_cen = f[pynbody.filt.Sphere('500 pc')]
        js, jv = self.getvals(f_cen)

        #f_cen = f[pynbody.filt.Sphere('1000 pc')]
        #js0, jv0 = self.getvals(f_cen)

        #f_cen = f[pynbody.filt.Sphere(r0)]
        #jsM, jvM = self.getvals(f_cen)

        js1, jv1 = self.getvals(f_marked.dm)

        f['vel'] += velmean
        f['pos'] += exist['SSC']

        return js, jv, js1, jv1


class Anisotropy(HaloProperties):
    # include

    def calculate(self, halo, existing_properties):
        halo["pos"] -= existing_properties["SSC"]
        vcen = halo[pynbody.filt.Sphere("1 kpc")].mean_by_mass("vel")
        halo["vel"] -= vcen

        halo.dm["vani"] = (halo.dm["vr"] ** 2) / halo.dm["v2"]

        x = np.where(halo.dm["v2"] > 0)

        pro = pynbody.analysis.profile.Profile(halo.dm[x], type='log',
                                               nbins=100,
                                               min=0.05, max=20.00)
        ret = pro["vani"]

        halo["vel"] += vcen
        halo["pos"] += existing_properties['SSC']

        return ret

    @classmethod
    def name(self):
        return "dm_vel_ani"





class HaloVelDispersionProfile(HaloProperties):


    @classmethod
    def plot_x0(cls):
        return 0.05

    @classmethod
    def plot_ylog(cls):
        return False

    @classmethod
    def plot_yrange(cls):
        return (None,None,None,(-1,1))

    @classmethod
    def plot_xdelta(cls):
        return 0.1

    @classmethod
    def plot_xlabel(cls):
        return "r/kpc"

    @classmethod
    def name(self):
        return "dm_mean_vel","dm_sigma_r", "dm_sigma_t", "dm_beta"

    def spherical_region(self):
        return True

    def rstat(self, halo, maxrad, cen, delta=0.1):
        halo['pos'] -= cen
        halo.wrap()
        vcen = halo[pynbody.filt.Sphere('%f kpc'%(delta*10))].mean_by_mass('vel')

        halo['vel'] -= vcen

        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)
        pro = pynbody.analysis.profile.Profile(halo, type='lin', ndim=3,
                                               min=0, max=maxrad, nbins=nbins)

        meanv, sigr, sigt, beta = np.sqrt(pro['v2_mean']), pro['vr_disp'], \
                           pro['vt_disp'], pro['beta']

        halo['vel'] += vcen
        halo['pos'] += cen
        halo.wrap()
        return meanv, sigr, sigt, beta

    def calculate(self, halo, existing_properties):
        return self.rstat(halo.dm, existing_properties["Rvir"],
                          existing_properties["SSC"], existing_properties.get('delta',0.1))


class HaloStarVelDispersionProfile(HaloVelDispersionProfile):
    # include

    @classmethod
    def name(self):
        return "st_mean_vel","st_sigma_r", "st_sigma_t", "st_beta"


    def calculate(self, halo, existing_properties):
        return self.rstat(halo.star, existing_properties["rmax_dm_local"]*5,
                          existing_properties["SSC"], existing_properties.get('delta',0.1))

class DynamicalDensityProfile(HaloProperties):

    @classmethod
    def name(self):
        return "dm_dynamical_density_profile"

    def spherical_region(self):
        return True

    def requires_property(self):
        return ["Sub"] + HaloProperties.requires_property(self)

    def accept(self, h_db):
        return (h_db.NDM > 20000) and (h_db.get("Sub", -1) == 0)

    def calculate(self, halo, existing_properties):
        import dyn_profile
        import copy
        print existing_properties["Sub"], existing_properties.NDM
        # if len(halo.dm)<10000 or existing_properties["Sub"]!=0 :
        # don't bother me with these small-fry
        #    raise ValueError, "Only interested in big halos"
        halo = copy.deepcopy(halo)
        worker = dyn_profile.DynamicalProfile(halo)
        MP = worker.mass_profile(1, threads=4, return_lower_bound=False)
        return worker.mass_to_rho(MP)

class JofE(HaloProperties):
    @classmethod
    def name(self):
        return "Evals","<j>(Evals)", "Evals_theory","<j>(Evals)_theory"

    def requires_property(self):
        return []

    def calculate(self, halo, existing_properties, fam=pynbody.family.dm):


        eps = existing_properties.get('delta',0.1)
        with pynbody.analysis.halo.center(halo, cen_size=eps*10):
            vrad = 100.0 # existing_properties['rmax_dm_local']*5 #pynbody.analysis.halo.virial_radius(halo.ancestor)

            fdp = dp.DynamicalProfile(halo.ancestor, subsample=2, eps=eps, max_r=vrad*3, \
                                  max_r_ptcls=vrad, vacuum=True, centre=False,
                                  ptcls=halo.ancestor[fam][pynbody.filt.Sphere(vrad*3)])

            Emax = float(fdp.ptcl_E.max())
            Jmax = float(fdp.ptcl_j.max())*3

            fdp.setup_E_grid(bins=100,Jr_max=Jmax,j_max=Jmax)
            fdp.interpolate_Jr_array()

            Es, js, Es2, js2 = dp.make_jE_plot(fdp,sub=halo,ptcls=200,plot=False)
            return Es, js, Es2, js2

class JofEStars(JofE):
    @classmethod
    def name(cls):
        return ["st_"+x for x in JofE.name()]

    def calculate(self, halo, existing_properties):
        return JofE.calculate(self,halo,existing_properties,pynbody.family.star)

class JProportion(HaloProperties):
    @classmethod
    def name(self):
        return "<E>","<j>"

    def requires_property(self):
        return []

    def calculate(self, halo, existing_properties):


        eps = existing_properties.get('delta',0.1)
        pynbody.analysis.halo.center(halo, cen_size=eps*10, wrap=True)
        vrad = pynbody.analysis.halo.virial_radius(halo.ancestor)

        fdp = dp.DynamicalProfile(halo.ancestor, subsample=2, eps=eps, max_r=vrad*3, \
                              max_r_ptcls=vrad, vacuum=True, centre=False)

        Emax = float(fdp.ptcl_E.max())
        Jmax = float(fdp.ptcl_j.max())*3

        fdp.setup_E_grid(bins=100,Jr_max=Jmax,j_max=Jmax)
        fdp.interpolate_Jr_array()

        jcirc = dp.ptcl_max_j(fdp)
        jcirc[jcirc==0] = fdp.ptcl_j[jcirc==0]
        jcirc[jcirc!=jcirc] = np.inf
        fdp.ptcls['j_j_circ'] = fdp.ptcl_j/jcirc

        fdp.ptcls['_E'] = fdp.ptcl_E

        self.fdp = fdp


        return np.mean(halo.dm['_E']),np.mean(halo.dm['j_j_circ'])


class HaloSpin(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "lambda_prime", "lambda_prime_dm"

    def requires_property(self):
        return HaloProperties.requires_property(self) + ["Vvir"]

    def lambda_prime(self, sim, prop):
        L_x = (
            sim["mass"] * (sim["y"] * sim["vz"] - sim["z"] * sim["vy"])).sum()
        L_y = (
            sim["mass"] * (sim["z"] * sim["vx"] - sim["x"] * sim["vz"])).sum()
        L_z = (
            sim["mass"] * (sim["x"] * sim["vy"] - sim["y"] * sim["vx"])).sum()
        L_tot = math.sqrt(L_x ** 2 + L_y ** 2 + L_z ** 2)
        mass = sim["mass"]
        try:
            return L_tot / (math.sqrt(2) * mass.sum() * prop["Rvir"] * prop["Vvir"])
        except ZeroDivisionError:
            return 0

    def calculate(self, halo, existing_properties):

        sub_sim_dm = halo.dm

        return self.lambda_prime(halo, existing_properties), self.lambda_prime(sub_sim_dm, existing_properties)


class RotCurve(HaloProperties):
    # include

    @classmethod
    def name(self):
        return "rotcurve", "rotcurve:range"

    def spherical_region(self):
        return True

    def requires_property(self):
        return "Rmax", "SSC"

    def calculate(self, halo, existing_properties):
        import copy

        maxr = existing_properties['Rmax']

        halo = halo[pynbody.filt.Sphere(maxr, cen=existing_properties['SSC'])]
        halo = copy.deepcopy(halo)
        halo['pos'] -= existing_properties['SSC']

        print "Center"
        pynbody.analysis.angmom.faceon(halo, top=halo, cen=(0, 0, 0))

        HX = halo
        targ_part = 500000

        if len(halo) > targ_part:
            skip = len(halo) / targ_part + 1

            print "Take every", skip, "th particle"
            halo = halo[::skip]
        else:
            skip = 1

        print "Mkprofile"
        pro = pynbody.analysis.profile.Profile(halo, type='log',
                                               nbins=100,
                                               min=0.05, max=maxr)
        print "V_circ"
        v = pro['v_circ'].in_units("km s^-1") * math.sqrt(skip)

        return np.array(list(v)), np.array([0.05, maxr])

@pynbody.analysis.profile.Profile.profile_property
def j_HI_enc(self):
    """
    Magnitude of the total angular momentum in HI as a function of distance from halo center
    """

    jx = np.zeros(self.nbins)
    jy = np.zeros(self.nbins)
    jz = np.zeros(self.nbins)
    jpx = 0.
    jpy = 0.
    jpz = 0.
    MHIenc = 0.
    for i in range(self.nbins):
        subs = self.sim[self.binind[i]]
        if len(subs)>0:
            MHIenc += (subs['mass'] * subs['HI']).sum()
            jpx += (subs['j'][:, 0] * subs['mass'] * subs['HI']).sum()
            jpy += (subs['j'][:, 1] * subs['mass'] * subs['HI']).sum()
            jpz += (subs['j'][:, 2] * subs['mass'] * subs['HI']).sum()
        if MHIenc > 0:
            jx[i] = jpx/MHIenc
            jy[i] = jpy/MHIenc
            jz[i] = jpz/MHIenc

    j_HI = np.concatenate(([jx],[jy],[jz])).T

    return j_HI

@pynbody.analysis.profile.Profile.profile_property
def j_enc(self):
    """
    Magnitude of total angular momentum
    """
    jx = np.zeros(self.nbins)
    jy = np.zeros(self.nbins)
    jz = np.zeros(self.nbins)
    jpx = 0.
    jpy = 0.
    jpz = 0.
    for i in range(self.nbins):
        subs = self.sim[self.binind[i]]
        if len(subs) > 0:
            jpx += (subs['j'][:, 0] * subs['mass']).sum()
            jpy += (subs['j'][:, 1] * subs['mass']).sum()
            jpz += (subs['j'][:, 2] * subs['mass']).sum()
        if self['mass_enc'][i] >0:
            jx[i] = jpx/self['mass_enc'][i]
            jy[i] = jpy/self['mass_enc'][i]
            jz[i] = jpz/self['mass_enc'][i]

    j = np.concatenate(([jx],[jy],[jz])).T

    return j


class AngMomEncl(HaloProperties):
    @classmethod
    def name(self):
        return "J_dm_enc", "J_gas_enc", "J_star_enc", "J_HI_enc"

    def requires_property(self):
        return ['SSC', 'Rvir']

    def rstat(self, halo, maxrad, delta=0.1):

        nbins = int(maxrad / delta)
        maxrad = delta * (nbins + 1)

        pro = pynbody.analysis.profile.Profile(halo.dm, type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
        J_dm = pro['j_enc']

        if len(halo.g) > 10:
            pro = pynbody.analysis.profile.Profile(halo.g, type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
            J_HI = pro['j_HI_enc']
            J_gas = pro['j_enc']

        else:
            J_HI = None
            J_gas = None

        if len(halo.s) > 10:
            pro = pynbody.analysis.profile.Profile(halo.s, type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
            J_star = pro['j_enc']

        else:
            J_star = None

        return J_dm, J_gas, J_star, J_HI

    def calculate(self,  halo, properties):
        com = properties['SSC']
        rad = properties['Rvir']
        halo["pos"] -= com
        halo.wrap()
        try:
            vcen = pynbody.analysis.halo.center_of_mass_velocity(halo.dm[pynbody.filt.Sphere('1 kpc')])
        except:
            vcen = pynbody.analysis.halo.center_of_mass_velocity(halo.dm[pynbody.filt.Sphere('2 kpc')])
        halo['vel'] -= vcen

        delta = properties.get('delta',0.1)
        J_dm, J_gas, J_star, J_HI = self.rstat(halo, rad, delta)
        halo["pos"] += com
        halo['vel'] += vcen
        halo.wrap()
        return J_dm, J_gas, J_star, J_HI

    @classmethod
    def plot_x0(cls):
        return 0.0

    @classmethod
    def plot_xdelta(cls):
        return 0.1

class AMdist(HaloProperties):
    @classmethod
    def name(self):
        return "AMdist_tot", "AMdist_dm", "AMdist_gas", "AMdist_star"

    def requires_property(self):
        return ['SSC']

    def cdist(self, sim, bins=50, max=5):
        jall = np.sqrt(np.sum(sim['j']**2,axis=1))
        jstar  =  np.sqrt(np.sum(sim.s['j']**2,axis=1))
        jgas = np.sqrt(np.sum(sim.g['j']**2,axis=1))
        jdm = np.sqrt(np.sum(sim.dm['j']**2,axis=1))
        jtot = np.sqrt(np.sum(np.sum(sim['j'],axis=0)**2))
        amdist_tot = np.histogram(jall/jtot,range=[0,max],density=True, bins=bins)
        amdist_dm = np.histogram(jdm/jtot,range=[0,max],density=True, bins=bins)
        amdist_gas = np.histogram(jgas/jtot,range=[0,max],density=True, bins=bins)
        amdist_star = np.histogram(jstar/jtot,range=[0,max],density=True, bins=bins)
        return amdist_tot, amdist_dm, amdist_gas, amdist_star

    def calculate(self, halo, properties):
        cen = properties['SSC']
        halo['pos'] -= cen

        try:
            vcen = pynbody.analysis.halo.center_of_mass_velocity(halo.dm[pynbody.filt.Sphere('1 kpc')])
        except:
            vcen = pynbody.analysis.halo.center_of_mass_velocity(halo.dm[pynbody.filt.Sphere('2 kpc')])

        halo['vel'] -= vcen
        amdist_tot, amdist_dm, amdist_gas, amdist_star = self.cdist(halo)
        halo['pos'] += cen
        halo['vel'] += vcen
        return amdist_tot, amdist_dm, amdist_gas, amdist_star

