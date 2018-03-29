from __future__ import absolute_import
from __future__ import print_function

import tangos as db
import tangos.core.timestep
from ..input_handlers import halo_stat_files as hsf
from .. import parallel_tasks
from ..log import logger
from . import GenericTangosTool


class PropertyImporter(GenericTangosTool):
    tool_name = 'import-properties'
    tool_description = 'Import a merger tree from the consistent-trees tool'

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')

        parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the halo-finder pre-calculated properties to import")

        parser.add_argument('--backwards', action='store_true',
                            help='Process low-z timesteps first')

    def process_options(self, options):
        self.options = options

    def run_calculation_loop(self):
        base_sim = db.sim_query_from_name_list(self.options.sims)

        names = self.options.properties

        for x in base_sim:
            timesteps = db.core.get_default_session().query(tangos.core.timestep.TimeStep).filter_by(
                simulation_id=x.id, available=True).order_by(tangos.core.timestep.TimeStep.redshift.desc()).all()

            if self.options.backwards:
                timesteps = timesteps[::-1]

            for ts in parallel_tasks.distributed(timesteps):
                logger.info("Processing %s",ts)
                hsf.HaloStatFile(ts).add_halo_properties(*names)
                db.core.get_default_session().commit()
