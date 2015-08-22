from simdb.tests import *

class TestSimsController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='sims', action='index'))
        # Test response...
