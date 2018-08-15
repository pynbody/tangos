from __future__ import absolute_import
from __future__ import print_function

import tangos as db
import tangos.core.timestep
from ..input_handlers import halo_stat_files as hsf
from .. import parallel_tasks
from ..log import logger
from .. import core, query, input_handlers
from . import GenericTangosTool


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

        parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the halo-finder pre-calculated properties to import")

        parser.add_argument('--backwards', action='store_true',
                            help='Process low-z timesteps first')

    def process_options(self, options):
        self.options = options

    def _import_properties_for_timestep(self, ts, property_names, object_typetag):
        """Import the named properties for a specific timestep

        :arg ts: the database timestep
        :arg property_names: list of names to import
        :arg object_typetag: the type tag of the objects for which properties will be imported

        :type ts: core.timestep.TimeStep
        """

        logger.info("Processing %s", ts)

        object_typecode = core.Halo.object_typecode_from_tag(object_typetag)
        all_objects = ts.objects.filter_by(object_typecode=object_typecode).all()

        finder_id_map = {}
        for h in all_objects:
            finder_id_map[h.finder_id] = h

        session = core.Session.object_session(ts)

        property_db_names = [core.dictionary.get_or_create_dictionary_item(session, name) for name in
                             property_names]
        property_objects = []
        for values in self.handler.get_object_properties_for_timestep(ts.extension, object_typetag, property_names):
            halo = finder_id_map.get(values[0], None)
            if halo is not None:
                for name_object, value in zip(property_db_names, values[1:]):
                    property_objects.append(core.halo_data.HaloProperty(halo, name_object, value))

        logger.info("Add %d properties", len(property_objects))
        session.add_all(property_objects)
        session.commit()

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
