"""A property calculated from particle data, from the custom properties tutorial.

docs/tutorials/custom_properties.rst includes this file, and imports it during the
documentation build; tests/test_docs_examples.py writes its property with
``tangos write``. Edit the three together.
"""

import numpy as np

from tangos.properties.pynbody import PynbodyPropertyCalculation


class VelocityDispersion(PynbodyPropertyCalculation):
    names = "velocity_dispersion"

    def calculate(self, particle_data, existing_properties):
        return np.std(particle_data['vel'])
