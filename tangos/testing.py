from __future__ import absolute_import
from __future__ import print_function
import tangos
from . import core
import sqlalchemy, sqlalchemy.event
import contextlib
import gc
import traceback
import os
import inspect
import six
from six.moves import range
from six.moves import zip

class TestSimulationGenerator(object):
    def __init__(self, sim_name="sim", session=None):
        if not session:
            session = core.get_default_session()

        self.session = session
        self.sim = tangos.core.simulation.Simulation(sim_name)
        self._ptcls_in_common_dict = core.dictionary.get_or_create_dictionary_item(self.session, "ptcls_in_common")
        self._BH_dict = core.dictionary.get_or_create_dictionary_item(self.session, "BH")
        self._host_dict = core.dictionary.get_or_create_dictionary_item(self.session, "host")
        self.session.commit()

    def add_objects_to_timestep(self, num_halos, object_typecode=0, NDM=None):
        """Add halos to the most recently added timestep"""

        ts = self._most_recently_added_timestep()
        returned_halos = []

        for i in range(1,num_halos+1):
            if NDM is None:
                NDM_halo = 1000-i*100
            else:
                NDM_halo = NDM[i-1]
            halo = tangos.core.halo.Halo(ts, i, i, NDM_halo, 0, 0, 0)
            halo.object_typecode = object_typecode
            returned_halos.append(self.session.merge(halo))

        self.session.commit()
        return returned_halos

    def add_properties_to_halos(self, **properties):
        """Add properties to the halos in the most recently added timestep.

        For each kwarg, pass in a function mapping the halo number to the named property

        e.g. add_properties_to_halos(Mvir=lambda i: 100.*(10-i)) adds a Mvir value of 100*(10-halo_number)
         to each halo"""

        self._add_properties_to_objects(0, **properties)

    def _add_properties_to_objects(self, htype, **properties):
        """Add properties to the halos in the most recently added timestep.

        For each kwarg, pass in a function mapping the halo number to the named property

        e.g. add_properties_to_halos(Mvir=lambda i: 100.*(10-i)) adds a Mvir value of 100*(10-halo_number)
         to each halo"""

        for k, v in six.iteritems(properties):
            for halo in self._most_recently_added_timestep().objects.filter_by(object_typecode=htype).all():
                halo[k] = v(halo.halo_number)

    def add_bhs_to_timestep(self, num_bhs):
        halos = self.add_objects_to_timestep(num_bhs, 1)
        return halos

    def add_properties_to_bhs(self, **properties):
        """Add properties to BHs. See add_properties_to_halos for documentation"""

        self._add_properties_to_objects(1, **properties)

    def _most_recently_added_timestep(self):
        return self.sim.timesteps[-1]

    def _two_most_recently_added_timesteps(self):
        return self.sim.timesteps[-2:]

    def add_timestep(self):
        """Add a sequentially-numbered timestep to the specified simulation"""

        timestep_num = len(self.sim.timesteps)+1
        ts = tangos.core.timestep.TimeStep(self.sim, "ts%d"%timestep_num)
        ts.redshift = 9 - timestep_num
        ts.time_gyr = 0.9*timestep_num
        self.session.add(ts)
        self.session.commit()

        return ts

    def link_last_halos_using_mapping(self, mapping, consistent_masses=True, object_typecode=0) :
        """Store halolinks such that ts_source[halo_i].next = ts_dest[mapping[halo_i]]

        :type mapping dict"""

        ts_source, ts_dest = self._two_most_recently_added_timesteps()
        if consistent_masses:
            self._adjust_halo_NDM_for_mapping(mapping, ts_dest, ts_source)
        self._generate_bidirectional_halolinks_for_mapping(mapping, ts_dest, ts_source, object_typecode)
        self.session.commit()

    def link_last_halos(self, object_typecode=0):
        """Generate default halolinks for the most recent two timesteps such that 1->1, 2->2 etc"""

        ts_source, ts_dest = self._two_most_recently_added_timesteps()
        halo_nums_source = set([a.halo_number for a in ts_source.objects.filter_by(object_typecode=object_typecode).all()])
        halo_nums_dest = set([a.halo_number for a in ts_dest.objects.filter_by(object_typecode=object_typecode).all()])
        halo_nums_common = halo_nums_source.intersection(halo_nums_dest)
        mapping = dict([(a,a) for a in halo_nums_common])
        self.link_last_halos_using_mapping(mapping,False, object_typecode)


    def link_last_bhs(self):
        self.link_last_halos(1)

    def link_last_bhs_using_mapping(self, mapping):
        self.link_last_halos_using_mapping(mapping, object_typecode=1)

    def assign_bhs_to_halos(self, mapping):
        ts = self._most_recently_added_timestep()

        for k,v in six.iteritems(mapping):
            source_halo = ts.halos.filter_by(halo_number=v).first()
            target_bh = ts.bhs.filter_by(halo_number=k).first()

            forward_link = core.halo_data.HaloLink(source_halo, target_bh, self._BH_dict,
                                                   1.0)
            self.session.add(forward_link)

            reverse_link = core.halo_data.HaloLink(target_bh, source_halo, self._host_dict,
                                                   1.0)
            self.session.add(reverse_link)

        self.session.commit()

    def link_last_halos_across_using_mapping(self, other, mapping, object_typecode=0):
        ts_source = self._most_recently_added_timestep()
        ts_dest = other._most_recently_added_timestep()
        self._generate_bidirectional_halolinks_for_mapping(mapping, ts_dest, ts_source, object_typecode)
        self.session.commit()

    def add_mass_transfer(self, source_num, target_num, fraction_of_source, object_typecode=0):
        ts_source, ts_dest = self._two_most_recently_added_timesteps()
        source_halo = ts_source.objects.filter_by(halo_number=source_num, object_typecode=object_typecode).first()
        target_halo = ts_dest.objects.filter_by(halo_number=target_num, object_typecode=object_typecode).first()

        transferred_ptcls = int(source_halo.NDM * fraction_of_source)
        fraction_of_dest = float(transferred_ptcls)/target_halo.NDM
        ts_source, ts_dest = self._two_most_recently_added_timesteps()

        forward_link = core.halo_data.HaloLink(source_halo, target_halo, self._ptcls_in_common_dict, fraction_of_source)
        backward_link = core.halo_data.HaloLink(target_halo, source_halo, self._ptcls_in_common_dict, fraction_of_dest)


    def _generate_bidirectional_halolinks_for_mapping(self, mapping, ts_dest, ts_source, object_typecode):

        for source_num, target_num in six.iteritems(mapping):
            source_halo = ts_source.objects.filter_by(halo_number=source_num, object_typecode=object_typecode).first()
            target_halo = ts_dest.objects.filter_by(halo_number=target_num, object_typecode=object_typecode).first()

            forward_link = core.halo_data.HaloLink(source_halo, target_halo, self._ptcls_in_common_dict, 1.0)
            backward_link = core.halo_data.HaloLink(target_halo, source_halo, self._ptcls_in_common_dict, float(source_halo.NDM) / target_halo.NDM)

            self.session.add_all([forward_link, backward_link])


    def _adjust_halo_NDM_for_mapping(self, mapping, ts_dest, ts_source):

        for source_num, target_num in six.iteritems(mapping):
            target_halo = ts_dest.halos.filter_by(halo_number=target_num).first()
            target_halo.NDM = 0
        for source_num, target_num in six.iteritems(mapping):
            source_halo = ts_source.halos.filter_by(halo_number=source_num).first()
            target_halo = ts_dest.halos.filter_by(halo_number=target_num).first()
            print(source_num, ts_source)
            target_halo.NDM += source_halo.NDM



def _as_halos(hlist, session=None):
    if session is None:
        session = core.get_default_session()
    rvals = []
    for h in hlist:
        if h is None:
            rvals.append(None)
        elif isinstance(h, core.halo.Halo):
            rvals.append(h)
        else:
            rvals.append(tangos.get_halo(h, session))
    return rvals

def _halos_to_strings(hlist):
    if len(hlist)==0:
        return "(empty list)"
    else:
        return str([hx.path if hx else "None" for hx in _as_halos(hlist)])

def halolists_equal(hl1, hl2, session=None):
    """Return True if hl1 and hl2 are equivalent lists of halos"""

    hl1 = _as_halos(hl1)
    hl2 = _as_halos(hl2)

    return len(hl1)==len(hl2) and all([h1==h2 for h1, h2 in zip(hl1,hl2)])

def assert_halolists_equal(hl1, hl2, session=None):
    equal = halolists_equal(hl1, hl2, session=None)
    assert equal, "Not equal: %s %s"%(_halos_to_strings(hl1),_halos_to_strings(hl2))

@contextlib.contextmanager
def autorevert():
    old_session = core.get_default_session()
    connection = core.get_default_engine().connect()
    transaction = connection.begin()
    isolated_session = core.Session(bind=connection)
    core.set_default_session(isolated_session)
    yield
    transaction.rollback()
    core.set_default_session(old_session)




@contextlib.contextmanager
def assert_connections_all_closed():
    num_connections = [0,0]
    connection_details = {}
    def on_checkout(dbapi_conn, connection_rec, connection_proxy):
        num_connections[0]+=1
        num_connections[1]+=1
        connection_details[id(connection_rec)] = traceback.extract_stack()

    def on_checkin(dbapi_conn, connection_rec):
        if id(connection_rec) in connection_details:
            num_connections[0]-=1
            del connection_details[id(connection_rec)]

    gc.collect()

    sqlalchemy.event.listen(core.get_default_engine().pool, 'checkout', on_checkout)
    sqlalchemy.event.listen(core.get_default_engine().pool, 'checkin', on_checkin)

    yield


    gc.collect()

    sqlalchemy.event.remove(core.get_default_engine().pool, 'checkout', on_checkout)
    sqlalchemy.event.remove(core.get_default_engine().pool, 'checkin', on_checkin)

    for k,v in six.iteritems(connection_details):
        print("object id",k,"not checked in; was created here:")
        for line in traceback.format_list(v):
            print("  ",line)

    assert num_connections[0]==0, "%d (of %d) connections were not closed"%(num_connections[0], num_connections[1])


def init_blank_db_for_testing(**init_kwargs):
    try:
        os.mkdir("test_dbs")
    except OSError:
        pass

    caller_fname = os.path.basename(inspect.getframeinfo(inspect.currentframe().f_back)[0])[:-3]

    print(caller_fname)
    db_name = "test_dbs/%s.db"%caller_fname
    try:

        os.remove(db_name)
    except OSError:
        pass

    core.init_db("sqlite:///"+db_name,**init_kwargs)

