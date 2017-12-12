# TODO: expand these tests!
import tangos, tangos.web
from tangos import testing
from webtest import TestApp
import numpy as np


def setup():
    testing.init_blank_db_for_testing()

    creator = tangos.testing.TestSimulationGenerator()

    halo_offset = 0
    for ts in range(1,4):
        creator.add_timestep()
        creator.add_objects_to_timestep(4)


    tangos.get_item("sim/ts1/halo_1")['test_image'] = np.zeros((500,500,3))
    tangos.get_default_session().commit()

    global app
    app = TestApp(tangos.web.main({}))


def test_root_page():
    response = app.get("/")
    assert response.status_int==200
    assert "table" in response

def test_simulation_page():
    response = app.get("/sim")
    assert response.status_int==200
    assert "table" in response

def test_timestep_page():
    response = app.get("/sim/ts1")
    assert response.status_int==200
    assert "table" in response

def test_halo_page():
    response = app.get("/sim/ts1/halo_1")
    assert response.status_int == 200
    assert "table" in response

def test_plot():
    response = app.get("/sim/ts1/halo_1/t()/vs/z().png")
    assert response.status_int == 200
    assert response.content_type == 'image/png'

def test_image_plot():
    response = app.get("/sim/ts1/halo_1/test_image.png")