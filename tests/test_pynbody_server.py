import os
import sys

import numpy as np
import numpy.testing as npt
import pynbody
import pytest

import tangos
import tangos.input_handlers.pynbody
import tangos.parallel_tasks as pt
import tangos.parallel_tasks.pynbody_server as ps
import tangos.parallel_tasks.pynbody_server.snapshot_queue
from tangos.parallel_tasks.pynbody_server import shared_object_catalogue
from tangos.testing import using_parallel_tasks


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


@using_parallel_tasks(3)
def test_get_array():
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


@using_parallel_tasks(3)
def test_get_shared_array():
    if pt.backend.rank()==1:
        shared_array = pynbody.array.array_factory((10,), int, True, True)
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


@using_parallel_tasks(3)
def test_get_shared_array_slice():
    """Like test_get_shared_array, but with a slice"""
    if pt.backend.rank()==1:
        shared_array = pynbody.array.array_factory((10,), int, True, True)
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

@using_parallel_tasks(2)
def test_simsnap_properties():
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


@using_parallel_tasks
def test_simsnap_arrays():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640")[test_filter]
    f_local.physical_units()
    assert (f['x'] == f_local['x']).all()
    assert (f.gas['iord'] == f_local.gas['iord']).all()

@using_parallel_tasks
def test_nonexistent_array():
    test_filter = pynbody.filt.Sphere('5000 kpc')
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(test_filter)
    with npt.assert_raises(KeyError):
        f['nonexistent']


@using_parallel_tasks
def test_halo_array():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.snapshot_queue.ObjectSpecification(0, 0))
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640").halos()[0]
    assert len(f)==len(f_local)
    assert (f['x'] == f_local['x']).all()
    assert (f.gas['temp'] == f_local.gas['temp']).all()


@pytest.mark.parametrize('shared_mem', [True, False])
@using_parallel_tasks
def test_remote_file_index(shared_mem):
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640", shared_mem=shared_mem)
    index_list = conn.get_index_list(ps.snapshot_queue.ObjectSpecification(0, 0))

    f_local = pynbody.load(tangos.config.base + "test_simulations/test_tipsy/tiny.000640").halos()[0]
    local_index_list = f_local.get_index_list(f_local.ancestor)

    assert (index_list==local_index_list).all()


def test_shared_file_index_uses_shared_obj_catalogue():
    @using_parallel_tasks
    def test():
        conn = ps.RemoteSnapshotConnection(handler, "tiny.000640", shared_mem=True)
        conn.get_index_list(ps.snapshot_queue.ObjectSpecification(0, 0))

    log = test()
    assert "Generating a shared object catalogue" in log

@pytest.mark.parametrize('mode', ['server', 'server-shared-mem'])
@using_parallel_tasks
def test_lazy_evaluation_is_local(mode):
    ts = handler.load_timestep("tiny.000640", mode=mode)
    f_local = pynbody.load(tangos.config.base+"test_simulations/test_tipsy/tiny.000640")
    f_local.physical_units()
    h_local = f_local.halos()

    remote_view = handler.load_object("tiny.000640", 0, 0, 'halo', mode=mode)
    comparator = h_local[0]

    centre_offset = (-6017.0,-123.8,566.4)
    remote_view['pos']-=centre_offset
    comparator['pos']-=centre_offset

    npt.assert_almost_equal(remote_view['x'], comparator['x'], decimal=4)

    # This is the critical test: if the lazy-evaluation of 'r' takes place on the server, it will not be using
    # the updated version of the position array. This is undesirable for two reasons: first, because the pynbody
    # snapshot seen by the client is inconsistent in a way that would never happen with a normal snapshot. Second,
    # because it means extra "derived" arrays are being calculated across the entire snapshot which we want to
    # avoid in a memory-bound situation.
    npt.assert_almost_equal(remote_view['r'], comparator['r'], decimal=4)

    ts.disconnect()

@using_parallel_tasks
def _test_no_repeat_failing_queries(mode):
    ts = handler.load_timestep("tiny.000640", mode=mode)
    f = handler.load_object("tiny.000640", 0, 0, 'halo', mode=mode)

    for i in range(10):
        # the following fails on the server (due to it being run with derived arrays off), but succeeds locally
        # what we are checking is that the server doesn't get pelted with requests for things it can't provide
        _ = f['r']
    ts.disconnect()

@pytest.mark.parametrize('mode', ['server', 'server-shared-mem'])
def test_no_repeat_failing_queries(mode):
    log = _test_no_repeat_failing_queries(mode)
    assert "processing 3 array fetches" in log
    # = 1 attempt to get on the simulation, 1 attempt to get at family level, then a final request for pos

@pynbody.snapshot.tipsy.TipsySnap.derived_quantity
def tipsy_specific_derived_array(sim):
    """Test derived array to ensure format-specific derived arrays are available"""
    return 1-sim['x']

@using_parallel_tasks
def test_underlying_class():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640")
    f = conn.get_view(ps.snapshot_queue.ObjectSpecification(0, 0))
    f_local = pynbody.load(tangos.config.base + "test_simulations/test_tipsy/tiny.000640").halos()[0]
    f_local.physical_units()
    npt.assert_almost_equal(f['tipsy_specific_derived_array'],f_local['tipsy_specific_derived_array'], decimal=4)
    assert f.connection.underlying_pynbody_class is pynbody.snapshot.tipsy.TipsySnap

@using_parallel_tasks
def test_correct_object_loading():
    """This regression test looks for a bug where the pynbody_server module assumed halos could be
    loaded just by calling f.halos() where f was the SimSnap. This is not true in general; for example,
    for SubFind catalogues one has both halos and groups and the correct arguments must be passed."""
    f_remote = handler.load_object('tiny.000640', 0, 0, mode='server')
    f_local = handler.load_object('tiny.000640', 0, 0, mode=None)
    assert (f_remote['iord']==f_local['iord']).all()
    f_remote = handler.load_object('tiny.000640', 0, 0, 'test-objects', mode='server')
    f_local = handler.load_object('tiny.000640', 0, 0, 'test-objects', mode=None)
    assert (f_remote['iord'] == f_local['iord']).all()

@using_parallel_tasks
def _test_region_loading(mode, expected_number_of_queries):
    f_remote = handler.load_region("tiny.000640", pynbody.filt.Sphere("3 Mpc"), mode=mode,
                                   expected_number_of_queries=expected_number_of_queries)
    f_local = handler.load_region("tiny.000640", pynbody.filt.Sphere("3 Mpc"), mode=None)
    assert (f_remote.dm['pos'] == f_local.dm['pos']).all()
    assert (f_remote.st['pos'] == f_local.st['pos']).all()

    if mode == 'server-shared-mem':
        shmem_view_has_kdtree = hasattr(handler.load_timestep('tiny.000640', mode=mode).shared_mem_view,'kdtree')
        if expected_number_of_queries > 20000:
            assert shmem_view_has_kdtree
        else:
            assert not shmem_view_has_kdtree

    # we trigger ANOTHER region load here, to check that the server doesn't get asked to
    # build the tree again
    f_remote = handler.load_region("tiny.000640", pynbody.filt.Sphere("2 Mpc"), mode=mode,
                                   expected_number_of_queries=expected_number_of_queries)

    del f_remote
    pt.barrier()

@pytest.mark.parametrize('mode', ['server', 'server-shared-mem'])
@pytest.mark.parametrize('expected_number_of_queries', [1, 100000])
def test_region_loading(mode, expected_number_of_queries):
    """This test ensures that a region can be loaded correctly under server mode"""
    log = _test_region_loading(mode, expected_number_of_queries)

    if expected_number_of_queries == 100000:
        assert log.count("Building KDTree") == 1
    else:
        assert "Building KDTree" not in log

@using_parallel_tasks
def test_oserror_on_nonexistent_file():
    with npt.assert_raises(OSError):
        f = ps.RemoteSnapshotConnection(handler, "nonexistent_file")

@pynbody.snapshot.tipsy.TipsySnap.derived_quantity
def metals(sim):
    """Derived array that will only be invoked for dm, since metals is present on disk for gas/stars"""
    return pynbody.array.SimArray(np.ones(len(sim)))

@pytest.mark.parametrize('mode', ['server', 'server-shared-mem'])
@using_parallel_tasks
def test_mixed_derived_loaded_arrays(mode):
    """Sometimes an array is present on disk for some families but is derived for others. A notable real-world example
    is the mass array for gas in ramses snapshots. Previously accessing this array in a remotesnap could cause errors,
    specifically a "derived array is not writable" error on the server. This test ensures that the correct behaviour"""

    f_remote = handler.load_object('tiny.000640', 0, 0, mode=mode)
    f_local = handler.load_object('tiny.000640', 0, 0, mode=None)
    assert (f_remote.dm['metals'] == f_local.dm['metals']).all()
    assert (f_remote.st['metals'] == f_local.st['metals']).all()



@pytest.mark.parametrize('load_sphere', [True, False])
@using_parallel_tasks(3)
def test_shmem_simulation(load_sphere):
    sphere_filter = pynbody.filt.Sphere("3 Mpc")
    def loader_function(**kwargs):
        if load_sphere:
            return handler.load_region("tiny.000640", sphere_filter, **kwargs)
        else:
            return handler.load_object("tiny.000640", 0, 0, **kwargs)
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



@using_parallel_tasks
def test_implict_array_promotion_shared_mem():

    f_remote = handler.load_timestep("tiny.000640", mode='server-shared-mem').shared_mem_view

    f_remote.dm['pos']
    f_remote.gas['pos']

    # Don't explicitly load the f_remote.star['pos']. It should implicitly get promoted:
    f_remote['pos']

    f_local = handler.load_timestep("tiny.000640", mode=None)
    assert (f_remote['pos'] == f_local['pos']).all()


@using_parallel_tasks
def test_explicit_array_promotion_shared_mem():

    f_remote = handler.load_timestep("tiny.000640", mode='server-shared-mem').shared_mem_view

    f_remote.dm['pos']
    f_remote.gas['pos']
    f_remote.star['pos']


    f_local = handler.load_timestep("tiny.000640", mode=None)
    assert (f_remote['pos'] == f_local['pos']).all()

def test_request_index_list_deserialization():
    o = ps.RequestIndexList(ps.snapshot_queue.ObjectSpecification(1, 2))
    tag, contents = o._tag, o.serialize()

    o2 = ps.Message.interpret_and_deserialize(tag, 0, contents)
    assert isinstance(o2, ps.RequestIndexList)

    assert o2.filter_or_object_spec == o.filter_or_object_spec


@using_parallel_tasks(3)
def test_transmit_receive_portable_catalogue():
    np.random.seed(1337)
    object_id_per_particle = np.array(np.random.randint(1, 10, 100))
    sim = pynbody.new(100)
    sim['iord'] = np.arange(100)
    sim['grp'] = object_id_per_particle
    halos = sim.halos() # this will create a catalogue from the grp array

    pt.barrier() # ensure initialization is complete

    if pt.backend.rank()==1:

        obj_cat = shared_object_catalogue.ReturnSharedObjectCatalog(halos)
        obj_cat.send(2)

        assert halos._index_lists.particle_index_list is not None
        assert hasattr(halos._index_lists.particle_index_list, '_shared_fname')

    else:
        halos_remote = shared_object_catalogue.ReturnSharedObjectCatalog.receive(1).attach_to_simulation(sim)
        for id_ in np.unique(object_id_per_particle):
            assert (halos[id_]['iord'] == halos_remote[id_]['iord']).all()

        assert hasattr(halos_remote._index_lists.particle_index_list, '_shared_fname')

    pt.barrier()

@using_parallel_tasks(3)
def test_server_generates_portable_catalogue():
    conn = ps.RemoteSnapshotConnection(handler, "tiny.000640", shared_mem=True)
    obj_cat = ps.shared_object_catalogue.get_shared_object_catalogue_from_server(conn.shared_mem_view, 'halo', 0)
    local_halo = handler.load_object("tiny.000640", 0, 0, mode=None)

    assert (obj_cat[0]['iord'] == local_halo['iord']).all()


def test_portable_catalogue_generated_only_once():
    log = test_server_generates_portable_catalogue() # runs on two processes, should only get one cat
    assert log.count("Generating a shared object catalogue for 'halo's") == 1
