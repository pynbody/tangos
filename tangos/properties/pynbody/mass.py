from . import PynbodyPropertyCalculation

class Masses(PynbodyPropertyCalculation):
    names = "finder_mass"

    def calculate(self, halo, existing_properties):
        return halo['mass'].sum()


class MassBreakdown(PynbodyPropertyCalculation):
    names = "finder_dm_mass", "finder_star_mass", "finder_gas_mass"
    
    def calculate(self, halo, existing_properties):
        return halo.dm['mass'].sum(), halo.star['mass'].sum(), halo.gas['mass'].sum()
