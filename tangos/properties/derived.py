"""Live properties derived by simple arithmetic from other stored properties.

Unlike the property modules under :mod:`tangos.properties.pynbody` or
:mod:`tangos.properties.yt`, the calculations here need no particle data and
no optional dependency -- they only combine values already in the database.
"""

import numpy as np

from . import LivePropertyCalculation

#: Gravitational constant, in units of kpc (km/s)^2 / Msol.
G = 4.30091e-6


class Vvir(LivePropertyCalculation):
    """The circular velocity at the virial radius, :math:`\\sqrt{G M_{vir} / R_{vir}}`.

    This assumes ``Mvir`` is stored in solar masses and ``Rvir`` in kpc, which
    is the convention used by the example property modules shipped with
    tangos; ``Vvir`` then comes out in km/s. A database whose ``Mvir`` or
    ``Rvir`` use different units will get a numerically wrong answer, since
    nothing here can detect that.
    """

    names = "Vvir"

    def requires_property(self):
        return ['Mvir', 'Rvir']

    def live_calculate(self, halo):
        return float(np.sqrt(G * halo['Mvir'] / halo['Rvir']))
