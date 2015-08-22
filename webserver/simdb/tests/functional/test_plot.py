from simdb.tests import *

class TestPlotController(TestController):

    def test_index(self):
        response = self.app.get(url(controller='plot', action='index'))
        # Test response...
