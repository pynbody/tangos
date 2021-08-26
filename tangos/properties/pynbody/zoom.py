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

        # If mass resolution is not present as a simulation property,
        # ensure backward compatibility by setting the min mass to infinity
        import numpy as np
        import pynbody.array as array
        simulation_min_dm_mass = array.SimArray(self.get_simulation_property("approx_resolution_Msol", default=np.inf),
                                                units="Msol")

        if np.isclose(loaded_data_min_dm_mass, simulation_min_dm_mass, rtol=1e-1, atol=1):
            # Loaded data contains some light particles. Tolerance of this test (agreement to 10% or to 1 Msol)
            # is sufficiently tight to separate heavy and light resolution,
            # while being broad enough to capture numerical inaccuracies coming
            # from the simulation mass resolution.
            n_heavy = (halo.dm['mass'] > loaded_data_min_dm_mass).sum()
            return float(n_heavy) / len(halo.dm)
        else:
            # Loaded data contains only "heavy" particles, e.g. an unzoomed halo in server mode
            return 1.0
