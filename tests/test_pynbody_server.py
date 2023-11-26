import os
import sys

import numpy as np
import numpy.testing as npt
import pynbody

import tangos
import tangos.input_handlers.pynbody
import tangos.parallel_tasks as pt
import tangos.parallel_tasks.pynbody_server as ps
import tangos.parallel_tasks.pynbody_server.snapshot_queue


class _TestHandler(tangos.input_handlers.pynbody.ChangaInputHandler):
    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        # Specialised object 'catalogue' to check this works ok when loading remotely
        if object_typetag=='test-objects' and mode is None:
            return self.load_timestep(ts_extension)[[finder_offset]]
        else:
            return super().load_object(ts_extension, finder_id, finder_offset, object_typetag, mode)

def setup_module():
    global handler
    pt.use("multiprocessing")
    tangos.config.base = os.path.dirname(__file__)+"/"
    handler = _TestHandler("test_simulations/test_tipsy")

def teardown_module():
    tangos.core.close_db()


def _get_array():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    for fname in pt.distributed(["tiny.000640", "tiny.000832"]):
        ps.snapshot_queue.RequestLoadPynbodySnapshot((handler, fname)).send(0)
        ps.snapshot_queue.ConfirmLoadPynbodySnapshot.receive(0)

        ps.RequestPynbodyArray(test_filter, "pos").send(0)

        f_local = pynbody.load(tangos.config.base+"/test_simulations/test_tipsy/"+fname)
        f_local.physical_units()
        remote_result =  ps.ReturnPynbodyArray.receive(0).contents
        assert (f_local[test_filter]['pos']==remote_result).all()

        ps.snapshot_queue.ReleasePynbodySnapshot().send(0)


def test_get_array():
    pt.use("multiprocessing-3")
    pt.launch(_get_array)


def _get_shared_array():
    if pt.backend.rank()==1:
        shared_array = pynbody.array._array_factory((10,), int, True, True)
        shared_array[:] = np.arange(0,10)
        pt.pynbody_server.transfer_array.send_array(shared_array, 2, True)
        assert shared_array[2]==2
        pt.barrier()
        # change the value, to be checked in the other process
        shared_array[2] = 100
        pt.barrier()
    elif pt.backend.rank()==2:
        shared_array = pt.pynbody_server.transfer_array.receive_array(2, True)
        assert shared_array[2]==2
        pt.barrier()
        # now the other process should be changing the value
        pt.barrier()
        assert shared_array[2]==100

def test_get_shared_array():
    pt.use("multiprocessing-3")
    pt.launch(_get_shared_array)

def _get_shared_array_slice():
    if pt.backend.rank()==1:
        shared_array = pynbody.array._array_factory((10,), int, True, True)
        shared_array[:] = np.arange(0,10)
        pt.pynbody_server.transfer_array.send_array(shared_array[1:7:2], 2, True)
        assert shared_array[3] == 3
        pt.barrier()
        # change the value, to be checked in the other process
        shared_array[3] = 100
        pt.barrier()
    elif pt.backend.rank()==2:
        shared_array = pt.pynbody_server.transfer_array.receive_array(2, True)
        assert len(shared_array)==3
        assert shared_array[1] == 3
        pt.barrier()
        # now the other process should be changing the value
        pt.barrier()
        assert shared_array[1]==100

def test_get_shared_array_slice():
    """Like test_get_shared_array, but with a slice"""
    pt.use("multiprocessing-3")
    pt.launch(_get_shared_array_slice)

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
    pt.use("multiprocessing-2")
    pt.launch(_test_simsnap_properties)


def _test_simsnap_arrays():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640")[test_filter]
    f_local.physical_units()
    assert (f['x'] == f_local['x']).all()
    assert (f.gas['iord'] == f_local.gas['iord']).all()

def test_simsnap_arrays():
    pt.use("multiprocessing-2")
    pt.launch(_test_simsnap_arrays)

def _test_nonexistent_array():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    with npt.assert_raises(KeyError):
        f['nonexistent']

def test_nonexistent_array():
    pt.use("multiprocessing-2")
    pt.launch(_test_nonexistent_array)


def _test_halo_array():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.snapshot_queue.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640").halos()[1]
    assert len(f)==len(f_local)
    assert (f['x'] == f_local['x']).all()
    assert (f.gas['temp'] == f_local.gas['temp']).all()

def test_halo_array():
    pt.use("multiprocessing-2")
    pt.launch(_test_halo_array)


def _test_remote_file_index():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.snapshot_queue.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640").halos()[1]
    local_index_list = f_local.get_index_list(f_local.ancestor)
    index_list = f['remote-index-list']
    assert (index_list==local_index_list).all()

def test_remote_file_index():
    pt.use("multiprocessing-2")
    pt.launch(_test_remote_file_index)

def _debug_print_arrays(*arrays):
    for vals in zip(*arrays):
        print(vals, file=sys.stderr)

def _test_lazy_evaluation_is_local():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.snapshot_queue.ObjectSpecification(1, 1))
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
    pt.use("multiprocessing-2")
    pt.launch(_test_lazy_evaluation_is_local)


@pynbody.snapshot.tipsy.TipsySnap.derived_quantity
def tipsy_specific_derived_array(sim):
    """Test derived array to ensure format-specific derived arrays are available"""
    return 1-sim['x']

def _test_underlying_class():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.snapshot_queue.ObjectSpecification(1, 1))
    f_local = pynbody.load(tangos.config.base + "test_simulations/test_tipsy/tiny.000640").halos()[1]
    f_local.physical_units()
    npt.assert_almost_equal(f['tipsy_specific_derived_array'],f_local['tipsy_specific_derived_array'], decimal=4)
    assert f.connection.underlying_pynbody_class is pynbody.snapshot.tipsy.TipsySnap

def test_underlying_class():
    pt.use("multiprocessing-2")
    pt.launch(_test_underlying_class)


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
    pt.use("multiprocessing-2")
    pt.launch(_test_correct_object_loading)



def _test_oserror_on_nonexistent_file():
    with npt.assert_raises(OSError):
        f = ps.RemoteSnapshotConnection(handler, "nonexistent_file")

def test_oserror_on_nonexistent_file():
    pt.use("multiprocessing-2")
    pt.launch(_test_oserror_on_nonexistent_file)



@pynbody.snapshot.tipsy.TipsySnap.derived_quantity
def metals(sim):
    """Derived array that will only be invoked for dm, since metals is present on disk for gas/stars"""
    return pynbody.array.SimArray(np.ones(len(sim)))

def _test_mixed_derived_loaded_arrays():
    f_remote = handler.load_object('tiny.000640', 1, 1, mode='server')
    f_local = handler.load_object('tiny.000640', 1, 1, mode=None)
    assert (f_remote.dm['metals'] == f_local.dm['metals']).all()
    assert (f_remote.st['metals'] == f_local.st['metals']).all()


def test_mixed_derived_loaded_arrays():
    """Sometimes an array is present on disk for some families but is derived for others. A notable real-world example
    is the mass array for gas in ramses snapshots. Previously accessing this array in a remotesnap could cause errors,
    specifically a "derived array is not writable" error on the server. This test ensures that the correct behaviour"""
    pt.use("multiprocessing-2")
    pt.launch(_test_mixed_derived_loaded_arrays)


def _test_shmem_simulation(load_sphere=False):
    sphere_filter = pynbody.filt.Sphere("3 Mpc")
    def loader_function(**kwargs):
        if load_sphere:
            return handler.load_region("tiny.000640", sphere_filter, **kwargs)
        else:
            return handler.load_object("tiny.000640", 1, 1, **kwargs)
    if pt.backend.rank()==1:
        f_remote = loader_function(mode='server-shared-mem')
        f_local = loader_function(mode=None)
        # note we are using the velocity rather than the position because the position is already accessed
        # in the case of the sphere region test. We intentionally load information family-by-family (see
        # below). Using a 3d array slice (vx, rather than vel) tests that we don't accidentally just retireve
        # 1d slices - the whole 3d array should be retrieved, even though the code only asks for the x component.
        assert (f_remote.dm['vx'] == f_local.dm['vx']).all()
        assert (f_remote.st['vx'] == f_local.st['vx']).all()

        f_remote.dm['vx'][:] = 1337.0 # this should be a copy, not the actual shared memory array

        assert 'vel' in f_remote.dm.keys() # should have got the whole 3d array
        pt.barrier()
        # other rank will test that 1337.0 is *not* in the array. The reason this must be
        # true is so that we don't get race conditions when two processes are processing overlapping
        # regions
        pt.barrier()
        f = handler.load_timestep("tiny.000640", mode='server-shared-mem').shared_mem_view
        # now we get the *actual* shared memory view, so updates here really should reflect into the
        # other process. This isn't particularly a desirable behaviour, but it just serves to verify
        # everything really is backing onto shared memory
        f.dm['vx'][:] = 1234.0
        pt.barrier()

        # We now want to test what happens when we load the rest of the position array. What we don't
        # want to happen is a 'local promotion' -- this would imply a copy into local memory, defeating
        # the point of having shared memory mode. Instead, we want to recognise that actually we always had
        # pointers into a simulation-level shared memory array, and just keep looking at that.

        assert 'vel' not in f.keys() # currently a family array
        assert f.dm['vel'].ancestor._shared_fname is not None

        # prompt a promotion:
        f.gas['vx']

        assert 'vel' in f.keys()
        assert f['vel']._shared_fname is not None

        # Note: in principle, there could arise a situation where the server 'promotes' the array and so unlinks
        # the shared memory file, but one or more clients still has a reference to it. However, the OS should
        # not actually delete the file until all references are closed, so this should not cause a crash - it just
        # means there could be excess memory usage. For now, let's not worry about it.


    elif pt.backend.rank()==2:
        pt.barrier() # let the other rank try to corrupt things
        f_remote = loader_function(mode='server-shared-mem')
        assert np.all(f_remote.dm['vx'] != 1337.0)
        pt.barrier()
        # other process is updating the shared memory array
        pt.barrier()
        f = handler.load_timestep("tiny.000640", mode='server-shared-mem').shared_mem_view
        assert np.all(f.dm['vx']==1234.0)


def test_shmem_simulation_with_halo():
    """This test ensures that a simulation can be loaded correctly in shared memory, and halos accessed"""
    pt.use("multiprocessing-3")
    pt.launch(_test_shmem_simulation)

def test_shmem_simulation_with_filter():
    """This test ensures that a simulation can be loaded correctly in shared memory, and filter regions accessed"""
    pt.use("multiprocessing-3")
    pt.launch(lambda: _test_shmem_simulation(load_sphere=True))

