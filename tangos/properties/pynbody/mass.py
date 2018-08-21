from . import PynbodyPropertyCalculation

class Masses(PynbodyPropertyCalculation):
    names = "finder_mass"

    def calculate(self, halo, existing_properties):
        return halo['mass'].sum()

