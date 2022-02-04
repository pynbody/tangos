from .. import core, all_simulations, get_simulation, get_timestep, get_object
from ..log import logger
import six
import numpy.testing as npt
from sqlalchemy.orm import joinedload, object_session, undefer, Session
import numpy as np

class TangosDbDiff(object):
    """Class to compare two databases, used by the tangos diff command line tool"""

    def __init__(self, uri1, uri2, max_objects=10, ignore_keys=[]):
        if isinstance(uri1, Session):
            self.session1 = uri1
            uri1 = str(uri1.connection().engine)
        else:
            core.init_db(uri1)
            self.session1 = core.get_default_session()


        if isinstance(uri2, Session):
            self.session2 = uri2
            uri2 = str(uri2.connection().engine)
        else:
            core.init_db(uri2)
            self.session2 = core.get_default_session()

        self.test_simulations = True
        self.test_timesteps = True
        self.test_objects = True
        self.max_objects = max_objects
        self.test_properties = True
        self.ignore_keys = ignore_keys
        self.failed = False

        logger.info("Database 1 = %s",uri1)
        logger.info("Database 2 = %s",uri2)
        logger.info("For each timestep will check data in first %d objects of each type",max_objects)
        if len(ignore_keys)>0:
            logger.info("The value of properties with the names %s will be ignored.",", ".join(ignore_keys))
            logger.info("Only the existence and type of these properties will be checked.")

    def fail(self, message, *args):
        logger.error(message, *args)
        self.failed = True

    def compare(self):
        sims1 = set([s.basename for s in all_simulations(self.session1)])
        sims2 = set([s.basename for s in all_simulations(self.session2)])

        if len(sims1-sims2)>0:
            self.fail("Database 1 has additional simulations: %s", list(sims1-sims2))
        if len(sims2-sims1)>0:
            self.fail("Database 2 has additional simulations: %s", list(sims2-sims1))

        if self.test_simulations:
            for sim in sims1.intersection(sims2):
                self.compare_simulation(sim)

    def compare_simulation(self, sim):
        logger.info("Comparing simulation %s",sim)
        sim1 = get_simulation(sim, self.session1)
        sim2 = get_simulation(sim, self.session2)

        ts1 = set([ts.extension for ts in sim1.timesteps])
        ts2 = set([ts.extension for ts in sim2.timesteps])

        self._check_same_set(ts1, ts2, str(sim), 'timesteps')

        if self.test_timesteps:
            for ts in ts1.intersection(ts2):
                self.compare_timestep(sim+"/"+ts)

    def compare_timestep(self, ts):
        logger.info("Comparing timestep %s", ts)
        ts1 = get_timestep(str(ts), self.session1)
        ts2 = get_timestep(str(ts), self.session2)

        obj_filter = (core.SimulationObjectBase.halo_number < self.max_objects) | (core.SimulationObjectBase.object_typecode == core.SimulationObjectBase.object_typecode_from_tag('bh'))

        objects1 = dict([(o.path, o) for o in ts1.objects.filter(obj_filter).all()])
        objects2 = dict([(o.path, o) for o in ts2.objects.filter(obj_filter).all()])
        self._check_same_set(objects1.keys(), objects2.keys(), str(ts), 'objects')


        if self.test_objects:
            keys = set(objects1.keys()).intersection(objects2.keys())
            for k in keys:
                self._compare_objects(objects1[k], objects2[k])


    def _joined_links_load(self, parent):
        return object_session(parent).query(core.HaloLink).with_parent(parent, "all_links").\
            options(joinedload("halo_to"))

    def _joined_properties_load(self, parent):
        return object_session(parent).query(core.HaloProperty).with_parent(parent, "all_properties").\
            options(undefer("*")).options(joinedload("name"))

    def compare_object(self, obj):
        obj1 = get_object(obj, self.session1)
        obj2 = get_object(obj, self.session2)
        self._compare_objects(obj1, obj2)

    def _compare_objects(self, obj1, obj2):
        properties1 = dict([(prop.name.text, prop.data_raw) for prop in self._joined_properties_load(obj1)])
        properties2 = dict([(prop.name.text, prop.data_raw) for prop in self._joined_properties_load(obj2)])

        self._check_dict_same(properties1, properties2, obj1.path)

        links1 = dict([(link.relation.text+"->"+link.halo_to.path, link.weight) for link in self._joined_links_load(obj1)])
        links2 = dict([(link.relation.text+"->"+link.halo_to.path, link.weight) for link in self._joined_links_load(obj2)])

        self._check_dict_same(links1, links2, obj1.path, 'links')

    def _check_dict_same(self, properties1, properties2, name_of_object, name_of_things='properties'):
        prop1_names = set(properties1.keys())
        prop2_names = set(properties2.keys())
        self._check_same_set(prop1_names, prop2_names, name_of_object, name_of_things)
        self._check_almost_equal(properties1, properties2, name_of_object)

    def _check_same_set(self, objects1, objects2, name_of_object, name_of_things):
        objects1 = set(objects1)
        objects2 = set(objects2)
        self._check_setdiff_null("Database 1", objects1, objects2, name_of_object, name_of_things)
        self._check_setdiff_null("Database 2",objects2, objects1, name_of_object, name_of_things)

    def _check_setdiff_null(self, name_of_db_1, objects1, objects2,  name_of_object, name_of_things):
        if len(objects1 - objects2) > 0:
            self.fail("When comparing %s, %s has %d additional %s", name_of_object, name_of_db_1,
                      len(objects1 - objects2), name_of_things)
            self._print_objects(objects1 - objects2)

    def _print_objects(self, objects):
        for o in objects:
            logger.info("  %s", o)

    def _check_almost_equal(self, dict1, dict2, name_of_object):
        for d in dict1.keys():
            if d in dict2:
                v1 = dict1[d]
                v2 = dict2[d]
                if d in self.ignore_keys:
                    self._check_properties_compatible(v1, v2, d, name_of_object)
                else:
                    self._check_properties_equal(v1, v2, d, name_of_object)

    def _check_properties_equal(self, v1, v2, d, name_of_object):
        try:
            self._assert_equal(v1, v2)
        except AssertionError as e:
            self.fail("When comparing %s, value mismatch for key %s", name_of_object, d)
            self.fail("%s", e)

    def _check_properties_compatible(self, v1, v2, name_of_property, name_of_object):
        if type(v1) != type(v2):
            self.fail("When comparing %s, types of key %s differ", name_of_object, name_of_property)
        else:
            if isinstance(v1, np.ndarray):
                if v1.shape != v2.shape:
                    self.fail("When comparing %s, array shapes of key %s differ", name_of_object, name_of_property)

    def _assert_equal(self, v1, v2):
        if v1 is None or v2 is None:
            assert v1 is None, "Value 2 is None but Value 1 is not None"
            assert v2 is None, "Value 1 is None but Value 2 is not None"
        elif isinstance(v1, six.string_types):
            assert v1 == v2
        elif isinstance(v1, tuple):
            for e1, e2 in zip(v1,v2):
                self._assert_equal(e1, e2)
        else:
            npt.assert_allclose(v1, v2, rtol=1e-3, atol=1e-3)
