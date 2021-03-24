from __future__ import absolute_import
from __future__ import print_function

from .. import parallel_tasks
from ..log import logger
from .. import core
from . import GenericTangosTool
from ..util import proxy_object
from ..util import timestep_object_cache
import numpy as np

class PropertyImporter(GenericTangosTool):
    tool_name = 'import-properties'
    tool_description = 'Import properties that were calculated by the halo finder'

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')

        parser.add_argument('--type', action='store', type=str, dest='typetag', default='halo',
                            help="Specify the object type to run on by tag name (e.g. 'halo' or 'group')")

        parser.add_argument('properties', action='store', nargs='*',
                            help="The names of the halo-finder pre-calculated properties to import; if not specified, all available properties are imported.")

        parser.add_argument('--backwards', action='store_true',
                            help='Process low-z timesteps first')

    def process_options(self, options):
        self.options = options

    def _create_property(self, name, object, value):
        """Create a single database property corresponding to the given value

        See _create_properties for more information."""
        if isinstance(value, proxy_object.ProxyObjectBase):
            value = value.relative_to_timestep_cache(self._object_cache).resolve(self._session)
            if value is not None:
                return core.halo_data.HaloLink(object, value, name)
        elif np.issubdtype(type(value), np.float) or np.issubdtype(type(value), np.integer):
            return core.halo_data.HaloProperty(object, name, value)
        elif value is not None:
            logger.warn("Ignoring stat file entry key='%s' value='%s' as the value is not a number",
                        name.text, value)

        return None

    def _create_properties(self, name, object, values):
        """Create database property or properties corresponding to the given values.

        The values can be proxy objects, to indicate a link should be created

        :arg name: the name ORM object
        :arg object: the object with which the property should be associated
        :arg values: the value, or a list of values
        :returns: a list of objects to be added to the database (always a list, even if there is only one value)
        """

        if isinstance(values, list):
            objects = [self._create_property(name, object, v) for v in values]
        else:
            objects = [self._create_property(name, object, values)]

        return filter(lambda x: x is not None, objects)

    def _import_properties_for_timestep(self, ts, property_names, object_typetag):
        """Import the named properties for a specific timestep

        :arg ts: the database timestep
        :arg property_names: list of names to import, or empty list to import all available names
        :arg object_typetag: the type tag of the objects for which properties will be imported

        :type ts: core.timestep.TimeStep
        """

        logger.info("Processing %s", ts)

        if len(property_names)==0:
            property_names = self.handler.available_object_property_names_for_timestep(ts.extension, object_typetag)

        self._object_cache = timestep_object_cache.TimestepObjectCache(ts)
        self._session = core.Session.object_session(ts)

        property_db_names = [core.dictionary.get_or_create_dictionary_item(self._session, name) for name in
                             property_names]
        rows_to_store = []
        for values in self.handler.iterate_object_properties_for_timestep(ts.extension, object_typetag, property_names):
            db_object = self._object_cache.resolve(values[0], object_typetag)
            if db_object is not None:
                for db_name, value in zip(property_db_names, values[1:]):
                    rows_to_store+=self._create_properties(db_name, db_object, value)


        logger.info("Add %d properties", len(rows_to_store))
        self._session.add_all(rows_to_store)
        self._session.commit()

    def run_calculation_loop(self):
        base_sim = core.sim_query_from_name_list(self.options.sims)

        names = self.options.properties
        object_typetag = self.options.typetag
    
        for x in base_sim:
            timesteps = core.get_default_session().query(core.timestep.TimeStep).filter_by(
                simulation_id=x.id, available=True).order_by(core.timestep.TimeStep.redshift.desc()).all()

            if self.options.backwards:
                timesteps = timesteps[::-1]

            self.handler = x.get_output_handler()

            for ts in parallel_tasks.distributed(timesteps):
                self._import_properties_for_timestep(ts, names, object_typetag)
