from . import HaloProperties

class MeanGasTemperature(HaloProperties):
    @classmethod
    def name(self):
        return "mean_temp"

    def calculate(self, particle_data, properties):
        return particle_data.gas['temp'].mean()
