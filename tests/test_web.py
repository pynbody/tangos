import tangos, tangos.web
import tangos.testing.simulation_generator
from tangos import testing
from webtest import TestApp
import numpy as np
import csv
import json
from six import StringIO
from six.moves.urllib import parse

def setup():
    testing.init_blank_db_for_testing()

    creator = tangos.testing.simulation_generator.TestSimulationGenerator()

    halo_offset = 0
    for ts in range(1,4):
        creator.add_timestep()
        creator.add_objects_to_timestep(4)
        creator.link_last_halos()

    tangos.get_default_session().commit()
    tangos.get_item("sim/ts1/halo_1")['test_image'] = np.zeros((500,500,3))
    for ts in tangos.get_simulation(1).timesteps:
        for h in ts.halos:
            h['test_value'] = 1.0

    tangos.get_default_session().commit()

    creator.add_timestep()  # add ts 4
    creator.add_objects_to_timestep(3)
    tangos.get_default_session().commit()
    # now make the object have halo numbers which have a different ordering to the database IDs
    tangos.get_item("sim/ts4/1")['test_value'] = 1.0
    tangos.get_item("sim/ts4/2")['test_value'] = 2.0
    tangos.get_item("sim/ts4/3")['test_value'] = 3.0
    tangos.get_item("sim/ts4/1").halo_number = 10
    tangos.get_default_session().commit()


    creator = tangos.testing.simulation_generator.TestSimulationGenerator("simname/has/slashes")
    creator.add_timestep()
    creator.add_objects_to_timestep(1)
    creator.add_timestep()
    creator.add_objects_to_timestep(1)
    creator.link_last_halos()
    tangos.get_simulation(2).timesteps[0].halos[0]['test_value'] = 2.0
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

def test_plot_as_csv_timeseries():
    response = app.get("/sim/ts3/halo_1/test_value/vs/z().csv")
    assert response.status_int == 200
    assert response.content_type == 'text/csv'
    assert "filename=timeseries_test_value_vs_z().csv" in response.content_disposition
    csv_reader = csv.reader(StringIO(response.body.decode('utf-8')))
    csv_rows = list(csv_reader)
    assert csv_rows[0]==['test_value', 'z()']
    assert csv_rows[1]==['1.0','6.0']
    assert csv_rows[2]==['1.0', '7.0']
    assert csv_rows[3]==['1.0', '8.0']

def test_plot_as_csv_timestep():
    response = app.get("/sim/ts3/test_value/vs/halo_number().csv")
    assert response.status_int == 200
    assert response.content_type == 'text/csv'
    assert "filename=timestep_test_value_vs_halo_number().csv" in response.content_disposition
    csv_reader = csv.reader(StringIO(response.body.decode('utf-8')))
    csv_rows = list(csv_reader)
    assert csv_rows[0]==['test_value', 'halo_number()']
    assert csv_rows[1]==['1.0','1.0']
    assert csv_rows[2] == ['1.0', '2.0']
    assert csv_rows[3] == ['1.0', '3.0']
    assert csv_rows[4] == ['1.0', '4.0']

def test_image_plot():
    response = app.get("/sim/ts1/halo_1/test_image.png")
    assert response.status_int == 200
    assert response.content_type == 'image/png'

def test_json_gather_float():
    response = app.get("/sim/ts1/gather/halo/test_value.json")
    assert response.content_type == 'application/json'
    assert response.status_int == 200
    result = json.loads(response.body.decode('utf-8'))
    assert result['timestep']=='ts1'
    assert result['data_formatted']==["1.00", "1.00", "1.00", "1.00"]
    assert result['can_use_in_plot'] is True
    assert result['can_use_as_filter'] is False
    assert result['is_array'] is False

def test_json_gather_array():
    response = app.get("/sim/ts1/gather/halo/test_image.json")
    assert response.content_type == 'application/json'
    assert response.status_int == 200
    result = json.loads(response.body.decode('utf-8'))
    assert result['timestep']=='ts1'
    assert result['data_formatted'][0]=="Array"
    assert result['can_use_in_plot'] is False
    assert result['can_use_as_filter'] is False
    assert result['is_array'] is True

def test_json_gather_bool():
    response = app.get("/sim/ts1/gather/halo/has_property(test_image).json")
    assert response.content_type == 'application/json'
    assert response.status_int == 200
    result = json.loads(response.body.decode('utf-8'))
    assert result['timestep'] == 'ts1'
    assert result['data_formatted'] == ["True", "False", "False", "False"]
    assert result['can_use_in_plot'] is False
    assert result['can_use_as_filter'] is True
    assert result['is_array'] is False


def test_simulation_with_slash():
    response = app.get("/")
    assert "simname/has/slashes" in response
    simpage_response = response.click("simname/has/slashes")
    assert "Simulation: simname/has/slashes" in simpage_response
    ts_response = simpage_response.click("Go", index=1)
    assert "Timestep: ts1" in ts_response
    # Unfortunately timestep page is now generated in javascript, so we can
    # no longer test ts_response.click("Go")
    halo_response = app.get("/simname_has_slashes/ts1/1")
    assert "halo 1 of ts1" in halo_response
    calculate_url = halo_response.pyquery("#calculate_url").text()
    calculate_url = parse.unquote(calculate_url)
    assert "simname_has_slashes" in calculate_url
    halo_next_step_response = halo_response.click("\+1$").follow()
    assert "halo 1 of ts2" in halo_next_step_response


def test_ordering_as_expected():
    """Tests for an issue where data returned to the web interface was in a different order to the initial table,
    causing results to be displayed out of order"""
    assert (tangos.get_item("sim/ts4").calculate_all("halo_number()") == np.array([10,2,3])).all()
    assert (tangos.get_item("sim/ts4").calculate_all("halo_number()",order_by_halo_number=True) == np.array([2,3,10])).all()

    response = app.get("/sim/ts4/gather/halo/test_value.json")
    assert response.content_type == 'application/json'
    assert response.status_int == 200
    result = json.loads(response.body.decode('utf-8'))
    assert result['timestep'] == 'ts4'
    assert result['data_formatted'] == ["2.00", "3.00", "1.00"]
