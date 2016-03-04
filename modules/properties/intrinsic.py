from . import LiveHaloProperties

class IntrinsicProperties(LiveHaloProperties):
    @classmethod
    def name(cls):
        return "t","z","a","dbid"


    def live_calculate(self, halo, input_names):
        ts = halo.timestep
        return ts.time_gyr, ts.redshift, 1./(1.+ts.redshift), halo.id