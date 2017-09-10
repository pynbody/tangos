from . import HaloProperties
import pynbody

class SphericalRegionHaloProperties(HaloProperties):

    def region_specification(self, db_data):
        return pynbody.filt.Sphere(db_data['Rvir'], db_data['SSC'])

    def requires_property(self):
        return ["SSC", "Rvir"]+super(SphericalRegionHaloProperties, self).requires_property()

