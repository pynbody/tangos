# TODO: expand these tests!
import tangos.web
from webtest import TestApp

def setup():
    global app
    app = TestApp(tangos.web.main({}))


def test_root_page():
    response = app.get("/")
    assert response.status_int==200
    assert "table" in response