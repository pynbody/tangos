from . import PynbodyPropertyCalculation


class MeanGasProperties(PynbodyPropertyCalculation):
    names = "mean_temp", "mean_rho"

    def calculate(self, particle_data, properties):
        return particle_data.gas['temp'].mean(), particle_data.gas['rho'].mean().in_units("m_p cm^-3")
