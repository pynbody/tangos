from . import HaloProperties, ProxyHalo
import numpy as np
import math
import pynbody


class Subhalo(HaloProperties):

    @classmethod
    def name(self):
        return "Sub"

    def requires_property(self):
        return ["SSC", "Rvir"]

    @classmethod
    def requires_simdata(self):
        return False

    def preloop(self, sim, filename, property_array):
        self.proplist = property_array

    def calculate(self, halo, existing_properties):
        current_sc = existing_properties["SSC"]
        q = 0
        for i in self.proplist:
            if i.has_key("SSC") and i["SSC"] != None:
                q += 1
                diff = (
                    (np.subtract(i["SSC"], current_sc) ** 2).sum() ** 0.5) / i["Rvir"]
                if diff == 0:
                    break
                if diff < 1:
                    return ProxyHalo(q)
        return None
