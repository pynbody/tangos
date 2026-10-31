"""A property calculated from particle data, from the custom properties tutorial.

docs/tutorials/custom_properties.rst includes this file, and the documentation
build imports it; tests/test_docs_examples.py writes its property with
``tangos write``. Edit the three together.
"""

import numpy as np

from tangos.properties.pynbody import PynbodyPropertyCalculation


class VelocityDispersion(PynbodyPropertyCalculation):
    names = "velocity_dispersion"

    def calculate(self, particle_data, existing_properties):
        # the one-dimensional dispersion: the spread of each velocity
        # component about its mean, averaged over the three components
        variance_each_axis = particle_data['vel'].var(axis=0)
        return float(np.sqrt(variance_each_axis.mean()))
