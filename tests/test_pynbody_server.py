from __future__ import absolute_import
from __future__ import print_function
import tangos.parallel_tasks.pynbody_server as ps
import pynbody
import tangos.parallel_tasks as pt
import tangos.input_handlers.pynbody
import tangos
import numpy.testing as npt

import sys
import os
from six.moves import zip


class TestHandler(tangos.input_handlers.pynbody.ChangaInputHandler):
    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        # Specialised object 'catalogue' to check this works ok when loading remotely
        if object_typetag=='test-objects' and mode is None:
            return self.load_timestep(ts_extension)[[finder_offset]]
        else:
            return super(TestHandler, self).load_object(ts_extension, finder_id, finder_offset, object_typetag, mode)

def setup():
    global handler
    pt.use("multiprocessing")
    tangos.config.base = os.path.dirname(__file__)+"/"
    handler = TestHandler("test_simulations/test_tipsy")

def _get_array():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    for fname in pt.distributed(["tiny.000640", "tiny.000832"]):
        ps.RequestLoadPynbodySnapshot((handler, fname)).send(0)
        ps.ConfirmLoadPynbodySnapshot.receive(0)

        ps.RequestPynbodyArray(test_filter, "pos").send(0)

        f_local = pynbody.load(tangos.config.base+"/test_simulations/test_tipsy/"+fname)
        f_local.physical_units()
        remote_result =  ps.ReturnPynbodyArray.receive(0).contents
        assert (f_local[test_filter]['pos']==remote_result).all()

        ps.ReleasePynbodySnapshot().send(0)


def test_get_array():
    pt.launch(_get_array,3)


def _test_simsnap_properties():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640")[test_filter]
    f_local.physical_units()

    assert len(f)==len(f_local)
    assert len(f.dm)==len(f_local.dm)
    assert len(f.gas)==len(f_local.gas)
    assert len(f.star)==len(f_local.star)
    assert f.properties['boxsize']==f_local.properties['boxsize']


def test_simsnap_properties():
    pt.launch(_test_simsnap_properties,2)


def _test_simsnap_arrays():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640")[test_filter]
    f_local.physical_units()
    assert (f['x'] == f_local['x']).all()
    assert (f.gas['iord'] == f_local.gas['iord']).all()

def test_simsnap_arrays():
    pt.launch(_test_simsnap_arrays,2)

def _test_nonexistent_array():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    with npt.assert_raises(KeyError):
        f['nonexistent']

def test_nonexistent_array():
    pt.launch(_test_nonexistent_array, 2)


def _test_halo_array():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640").halos()[1]
    assert len(f)==len(f_local)
    assert (f['x'] == f_local['x']).all()
    assert (f.gas['temp'] == f_local.gas['temp']).all()

def test_halo_array():
    pt.launch(_test_halo_array, 2)


def _test_remote_file_index():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640").halos()[1]
    local_index_list = f_local.get_index_list(f_local.ancestor)
    index_list = f['remote-index-list']
    assert (index_list==local_index_list).all()

def test_remote_file_index():
    pt.launch(_test_remote_file_index, 2)

def _debug_print_arrays(*arrays):
    for vals in zip(*arrays):
        print(vals, file=sys.stderr)

def _test_lazy_evaluation_is_local():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640").halos()[1]
    f_local.physical_units()

    centre_offset = (-6017.0,-123.8,566.4)
    f['pos']-=centre_offset
    f_local['pos']-=centre_offset

    npt.assert_almost_equal(f['x'], f_local['x'], decimal=4)

    # This is the critical test: if the lazy-evaluation of 'r' takes place on the server, it will not be using
    # the updated version of the position array. This is undesirable for two reasons: first, because the pynbody
    # snapshot seen by the client is inconsistent in a way that would never happen with a normal snapshot. Second,
    # because it means extra "derived" arrays are being calculated across the entire snapshot which we want to
    # avoid in a memory-bound situation.
    npt.assert_almost_equal(f['r'], f_local['r'], decimal=4)

def test_lazy_evaluation_is_local():
    pt.launch(_test_lazy_evaluation_is_local, 2)


@pynbody.snapshot.tipsy.TipsySnap.derived_quantity
def tipsy_specific_derived_array(sim):
    """Test derived array to ensure format-specific derived arrays are available"""
    return 1-sim['x']

def _test_underlying_class():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base + "test_simulations/test_tipsy/tiny.000640").halos()[1]
    f_local.physical_units()
    npt.assert_almost_equal(f['tipsy_specific_derived_array'],f_local['tipsy_specific_derived_array'], decimal=4)
    assert f.connection.underlying_pynbody_class is pynbody.snapshot.tipsy.TipsySnap

def test_underlying_class():
    pt.launch(_test_underlying_class, 2)


def _test_correct_object_loading():
    f_remote = handler.load_object('tiny.000640', 1, 1, mode='server')
    f_local = handler.load_object('tiny.000640', 1, 1, mode=None)
    assert (f_remote['iord']==f_local['iord']).all()
    f_remote = handler.load_object('tiny.000640', 1, 1, 'test-objects', mode='server')
    f_local = handler.load_object('tiny.000640', 1, 1, 'test-objects', mode=None)
    assert (f_remote['iord'] == f_local['iord']).all()

def test_correct_object_loading():
    """This regression test looks for a bug where the pynbody_server module assumed halos could be
    loaded just by calling f.halos() where f was the SimSnap. This is not true in general; for example,
    for SubFind catalogues one has both halos and groups and the correct arguments must be passed."""
    pt.launch(_test_correct_object_loading, 2)