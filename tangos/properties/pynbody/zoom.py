from . import PynbodyPropertyCalculation


class Contamination(PynbodyPropertyCalculation):
    """ Calculates the contamination of a zoomed halo by heavier, unzoomed particles.
        The current behaviour returns:
            1.0 if the halo contains only heavy particles
            X.X if the halo contains a mixture of heavy and light particles
            0.0 if the halo contains exclusively light particles
        Heavies are defined as any particle heavier than the deepest zoom level if multiple zooms are performed
    """

    names = "contamination_fraction"

    def calculate(self, halo, exist):
        loaded_data_min_dm_mass = halo.dm['mass'].min()

        # If mass resolution not present as a property, ensure backward compatibility by setting the min mass to infinity
        import numpy as np
        simulation_min_dm_mass = self._simulation.get("approx_resolution_Msol", default=np.inf)

        if loaded_data_min_dm_mass > simulation_min_dm_mass:
            # Loaded data contains only "heavy" particles, e.g. an unzoomed halo in server mode
                return 1.0
        else:
            # Loaded data contains heavy and/or light particles, e.g
            n_heavy = (halo.dm['mass'] > loaded_data_min_dm_mass).sum()
            return float(n_heavy) / len(halo.dm)
