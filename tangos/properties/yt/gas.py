from . import YtPropertyCalculation


class GasMass(YtPropertyCalculation):
    """Calculates masses of total and cold (T<1.5x10^4 K) gas
    within a sphere of radius Rvir surrounding the center of a
    given halo"""
    names = "Mgas","Mcoldgas"

    def requires_property(self):
        return ["center", "Rvir_kpc"]

    def calculate(self, particle_data, existing_properties):
         cgas = particle_data.cut_region(["obj['temperature'] < 1.5e4"])
         return float(particle_data.quantities.total_quantity(('gas', 'cell_mass')).in_units('Msun').value),\
                float(cgas.quantities.total_quantity(('gas', 'cell_mass')).in_units('Msun').value)
