from tangos.properties import HaloProperties, TimeChunkedProperty
import numpy as np
import pynbody
from tangos.properties.centring import centred_calculation

kb = pynbody.array.SimArray(1.380658e-16, 'erg K**-1')
mh = pynbody.array.SimArray(1.6726219e-24, 'g')

def emissivity(rho, T, mu, tcool):
	return 3. / 2. * rho * kb * T / (mu * mh * tcool)


def tcool(rho, T, mu):
	# taken from https://arxiv.org/abs/astro-ph/9809159 eq 12
	fm = 1.0  # metalicity dependent factor, 1.0 for solar, 0.03 for pristine
	C1 = 3.88e11
	C2 = 5e7
	return C1 * mu * mh * T ** 0.5 / (rho * (1 + C2 * fm / T))


@pynbody.analysis.profile.Profile.profile_property
def Tew(self):
	temp = np.zeros(self.nbins)
	for i in range(self.nbins):
		subs = self.sim[self.binind[i]]
		#use = np.where(subs.g['temp'] > temp_cut)[0]
		mu = 0.58
		tc = tcool(subs.g['rho'].in_units('g cm**-3'), subs.g['temp'], mu)
		em = emissivity(subs.g['rho'].in_units('g cm**-3'), subs.g['temp'], mu, tc)
		temp[i] = np.sum(em * subs.g['temp']) / np.sum(em)

	return kb.in_units('keV K**-1') * temp


@pynbody.analysis.profile.Profile.profile_property
def Tmw(self):
	temp = np.zeros(self.nbins)
	for i in range(self.nbins):
		subs = self.sim[self.binind[i]]
		#use = np.where(subs.g['temp'] > temp_cut)[0]
		temp[i] = np.sum(subs.g['mass'] * subs.g['temp']) / np.sum(subs.g['mass'])
	return kb.in_units('keV K**-1') * temp

@pynbody.analysis.profile.Profile.profile_property
def rho_e(self):
	n_e = np.zeros(self.nbins)
	for i in range(self.nbins):
		subs = self.sim[self.binind[i]]
		#use = np.where(subs.g['temp'] > temp_cut)[0]
		n_e[i] = np.sum(subs.g['ne'] * subs.g['mass'].in_units('m_p'))/self._binsize.in_units('cm**'+str(int(self.ndim)))[i]
	return n_e


class GasProfiles(HaloProperties):
	_temp_cut = 1.26e6
	@classmethod
	def name(self):
		return "Tew_profile", "Tmw_profile", "rho_e_profile"

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
		return "T$_{ew}$ keV", "T$_{mw}$ keV"

	def requires_property(self):
		return ["shrink_center", "max_radius"]

	@centred_calculation
	def calculate(self, halo, existing_properties):
		#halo['pos'] -= existing_properties['SSC']
		#halo.wrap()
		delta = self.plot_xdelta()
		nbins = int(existing_properties['max_radius']/ delta)
		maxrad = delta * (nbins + 1)
		ps = pynbody.analysis.profile.Profile(halo.g[pynbody.filt.HighPass('temp',self._temp_cut)], type='lin', ndim=3, min=0, max=maxrad, nbins=nbins)
		Tew = ps['Tew']
		Tmw = ps['Tmw']
		rho_e = ps['rho_e']
		return Tew, Tmw, rho_e