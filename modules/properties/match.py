from . import LiveHaloProperties

class MatchHalos(LiveHaloProperties):

    @classmethod
    def name(self):
        return "match"

    def live_calculate(self, halo, array):
        return np.linalg.norm(array, axis=-1)