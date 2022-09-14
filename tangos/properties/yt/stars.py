from . import YtPropertyCalculation


class StellarMass(YtPropertyCalculation):
    """Returns mass of stars within Rvir of center of halo"""
    names = "Mstar"

    def requires_property(self):
        return ["center", "Rvir_kpc"]

    def calculate(self, particle_data, existing_properties):
        return float(particle_data.quantities.total_quantity(('stars', 'particle_mass')).in_units('Msun').value)
