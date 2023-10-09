from . import PynbodyHaloProperties
from .centring import centred_calculation


class Radius(PynbodyHaloProperties):

    @staticmethod
    def _get_overdensity_contrast():
        raise NotImplementedError("This is meant to be an abstract class")

    @classmethod
    def get_contrast(cls):
        return cls._get_overdensity_contrast()

    @staticmethod
    def _get_reference_definition():
        raise NotImplementedError("This is meant to be an abstract class")

    @classmethod
    def get_rhodef(cls):
        return cls._get_reference_definition()

    @centred_calculation
    def calculate(self, particle_data, existing_properties):
        import pynbody.analysis as analysis
        self._ensure_pynbody_mass_array_loaded_family_level(particle_data)

        # Virial radius is calculated using density contributions form all families
        return analysis.halo.virial_radius(particle_data, overden=self.get_contrast(), rho_def=self.get_rhodef())

    def region_specification(self, existing_properties):
        import pynbody
        return pynbody.filt.Sphere(existing_properties['max_radius'] * 3,
                                   existing_properties['shrink_center'])

    def requires_property(self):
        return ["shrink_center", "max_radius"]

    @staticmethod
    def _ensure_pynbody_mass_array_loaded_family_level(particle_data):
        # Make sure the mass array is loaded at family level
        import pynbody.family
        for family in particle_data.families():
            if family is pynbody.family.dm:
                particle_data.d['mass']
            if family is pynbody.family.star:
                particle_data.s['mass']
            if family is pynbody.family.gas:
                particle_data.g['mass']


class Radius200m(Radius):
    names = "r200m"

    @staticmethod
    def _get_overdensity_contrast():
        return 200

    @staticmethod
    def _get_reference_definition():
        return 'matter'


class Radius200c(Radius):
    names = "r200c"

    @staticmethod
    def _get_overdensity_contrast():
        return 200

    @staticmethod
    def _get_reference_definition():
        return 'critical'


class RadiusVirial(Radius):
    names = "rvirial"

    @staticmethod
    def _get_overdensity_contrast():
        return 178

    @staticmethod
    def _get_reference_definition():
        return 'matter'
