from . import PynbodyPropertyCalculation

class Contamination(PynbodyPropertyCalculation):

    def calculate(self, halo, exist):
        n_heavy = (halo.dm['mass'] > self.min_dm_mass).sum()
        return float(n_heavy) / len(halo.dm)

    def preloop(self, sim, db_timestep):
        self.min_dm_mass = sim.dm['mass'].min()

    names = "contamination_fraction"