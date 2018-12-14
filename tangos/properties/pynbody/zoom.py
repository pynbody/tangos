from . import PynbodyPropertyCalculation


class Contamination(PynbodyPropertyCalculation):
    """ Calculates the contamination of a zoomed halo by heavier, unzoomed particles.
        The current behaviour is to return 1.0 for halo containing no particles of the deepest zoom level,
        the mass fraction of heavy to light particles for halos containing a mixture of particle masses
        and 0.0 for halos containing exclusively particles from the deepest zoom level.
    """

    names = "contamination_fraction"

    def calculate(self, halo, exist):
        simulation_min_dm_mass = self._simulation.get("approx_resolution_Msol", 0.1)
        if self.loaded_data_min_dm_mass > simulation_min_dm_mass:
            # Loaded data contains only "heavy" particles, e.g. an unzoomed halo in server mode
                return 1.0
        else:
            # Loaded data contains heavy and/or light particles, e.g
            n_heavy = (halo.dm['mass'] > self.loaded_data_min_dm_mass).sum()
            return float(n_heavy) / len(halo.dm)

    def preloop(self, sim, db_timestep):
        self.loaded_data_min_dm_mass = sim.dm['mass'].min()

