import halo_db
from . import core
import sqlalchemy, sqlalchemy.event
import contextlib
import gc
import traceback

def add_symmetric_link(h1, h2, weight=1.0):
    """Add a bidirectional link between h1 and h2 with the specified weight"""
    rel = core.dictionary.get_or_create_dictionary_item(core.get_default_session(), "ptcls_in_common")
    core.get_default_session().add_all([core.halo_data.HaloLink(h1, h2, rel, weight), core.halo_data.HaloLink(h2, h1, rel, weight)])

class TestDatabaseGenerator(object):
    def __init__(self, sim_name="sim", session=None):
        if not session:
            session = core.get_default_session()

        self.session = session
        self.sim = halo_db.core.simulation.Simulation(sim_name)
        self._ptcls_in_common_dict = core.dictionary.get_or_create_dictionary_item(self.session, "ptcls_in_common")
        self.session.commit()

    def add_halos_to_timestep(self, num_halos, properties = {}, halo_type=0, NDM=None):
        """Add halos to the most recently added timestep, with the specified properties.

        Each property is a lambda function taking the halo number and returning the named property"""

        ts = self._most_recently_added_timestep()
        returned_halos = []

        for i in xrange(1,num_halos+1):
            if NDM is None:
                NDM_halo = 1000-i*100
            else:
                NDM_halo = NDM[i-1]
            halo = halo_db.core.halo.Halo(ts, i, NDM_halo, 0, 0, 0)
            halo.halo_type = halo_type
            returned_halos.append(self.session.merge(halo))
            for k,v in properties.iteritems():
                halo[k] = v(i)

        self.session.commit()
        return returned_halos

    def add_bhs_to_timestep(self, num_bhs, properties = {}):
        halos = self.add_halos_to_timestep(num_bhs, properties, 1)
        return halos

    def _most_recently_added_timestep(self):
        return self.sim.timesteps[-1]

    def _two_most_recently_added_timesteps(self):
        return self.sim.timesteps[-2:]

    def add_timestep(self):
        """Add a sequentially-numbered timestep to the specified simulation"""

        timestep_num = len(self.sim.timesteps)+1
        ts = halo_db.core.timestep.TimeStep(self.sim, "ts%d"%timestep_num, False)
        ts.redshift = 9 - timestep_num
        ts.time_gyr = 0.9*timestep_num
        self.session.add(ts)
        self.session.commit()

        return ts

    def link_last_halos_using_mapping(self, mapping, consistent_masses=True) :
        """Store halolinks such that ts_source[halo_i].next = ts_dest[mapping[halo_i]]

        :type mapping dict"""

        ts_source, ts_dest = self._two_most_recently_added_timesteps()
        if consistent_masses:
            self._adjust_halo_NDM_for_mapping(mapping, ts_dest, ts_source)
        self._generate_bidirectional_halolinks_for_mapping(mapping, ts_dest, ts_source)
        self.session.commit()

    def link_last_halos_across_using_mapping(self, other, mapping):
        ts_source = self._most_recently_added_timestep()
        ts_dest = other._most_recently_added_timestep()
        self._generate_bidirectional_halolinks_for_mapping(mapping, ts_dest, ts_source)
        self.session.commit()

    def add_mass_transfer(self, source_num, target_num, fraction_of_source):
        ts_source, ts_dest = self._two_most_recently_added_timesteps()
        source_halo = ts_source.halos.filter_by(halo_number=source_num).first()
        target_halo = ts_dest.halos.filter_by(halo_number=target_num).first()

        transferred_ptcls = int(source_halo.NDM * fraction_of_source)
        fraction_of_dest = float(transferred_ptcls)/target_halo.NDM
        ts_source, ts_dest = self._two_most_recently_added_timesteps()

        forward_link = core.halo_data.HaloLink(source_halo, target_halo, self._ptcls_in_common_dict, fraction_of_source)
        backward_link = core.halo_data.HaloLink(target_halo, source_halo, self._ptcls_in_common_dict, fraction_of_dest)


    def _generate_bidirectional_halolinks_for_mapping(self, mapping, ts_dest, ts_source):

        for source_num, target_num in mapping.iteritems():
            source_halo = ts_source.halos.filter_by(halo_number=source_num).first()
            target_halo = ts_dest.halos.filter_by(halo_number=target_num).first()

            forward_link = core.halo_data.HaloLink(source_halo, target_halo, self._ptcls_in_common_dict, 1.0)
            backward_link = core.halo_data.HaloLink(target_halo, source_halo, self._ptcls_in_common_dict, float(source_halo.NDM) / target_halo.NDM)

            self.session.add_all([forward_link, backward_link])


    def _adjust_halo_NDM_for_mapping(self, mapping, ts_dest, ts_source):

        for source_num, target_num in mapping.iteritems():
            target_halo = ts_dest.halos.filter_by(halo_number=target_num).first()
            target_halo.NDM = 0
        for source_num, target_num in mapping.iteritems():
            source_halo = ts_source.halos.filter_by(halo_number=source_num).first()
            target_halo = ts_dest.halos.filter_by(halo_number=target_num).first()
            print source_num, ts_source
            target_halo.NDM += source_halo.NDM



def _as_halos(hlist, session=None):
    if session is None:
        session = core.get_default_session()
    rvals = []
    for h in hlist:
        if isinstance(h, core.halo.Halo):
            rvals.append(h)
        else:
            rvals.append(halo_db.get_halo(h, session))
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
def assert_connections_all_closed():
    num_connections = [0,0]
    connection_details = {}
    def on_checkout(dbapi_conn, connection_rec, connection_proxy):
        num_connections[0]+=1
        num_connections[1]+=1
        connection_details[id(connection_rec)] = traceback.extract_stack()

    def on_checkin(dbapi_conn, connection_rec):
        num_connections[0]-=1
        del connection_details[id(connection_rec)]

    gc.collect()

    sqlalchemy.event.listen(core.get_default_engine().pool, 'checkout', on_checkout)
    sqlalchemy.event.listen(core.get_default_engine().pool, 'checkin', on_checkin)

    yield

    gc.collect()

    sqlalchemy.event.remove(core.get_default_engine().pool, 'checkout', on_checkout)
    sqlalchemy.event.remove(core.get_default_engine().pool, 'checkin', on_checkin)

    for k,v in connection_details.iteritems():
        print "object id",k,"not checked in; was created here:"
        for line in traceback.format_list(v):
            print "  ",line

    assert num_connections[0]==0, "%d (of %d) connections were not closed"%(num_connections[0], num_connections[1])
