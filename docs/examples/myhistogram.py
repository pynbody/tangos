"""A histogram property, from the histograms tutorial.

docs/tutorials/histograms.rst includes this file; tests/test_docs_examples.py
writes its property with ``tangos write``. Edit the three together.
"""

import numpy as np

from tangos.input_handlers.pynbody import PynbodyInputHandler
from tangos.properties import TimeChunkedProperty


class MetalsIntoStarsHistogram(TimeChunkedProperty):
    names = "metals_into_stars_histogram"
    works_with_handler = PynbodyInputHandler
    requires_particle_data = True

    def calculate(self, particle_data, existing_properties):
        stars = particle_data.star
        t_now = particle_data.properties['time'].in_units("Gyr")

        # the whole history, in bins of self.pixel_delta_t_Gyr from t=0 ...
        nbins = self.bin_index(t_now)
        metal_mass_in_each_star = stars['mass'].in_units("Msol") * np.asarray(stars['metals'])
        metal_mass, _ = np.histogram(stars['tform'].in_units("Gyr"), weights=metal_mass_in_each_star,
                                     bins=nbins, range=(0, nbins * self.pixel_delta_t_Gyr))
        rate = metal_mass / (self.pixel_delta_t_Gyr * 1e9)  # Msol per yr

        # ... of which only the part since the previous timestep is stored
        return rate[self.store_slice(t_now)]
