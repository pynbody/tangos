from __future__ import absolute_import
from __future__ import print_function

import tangos as db
import os
from ..input_handlers import ahf_trees as at
from ..log import logger
from ..core import get_or_create_dictionary_item
from ..core.halo_data import HaloLink, HaloProperty
from .. import config
from . import GenericTangosTool
from six.moves import xrange
import re

class AHFTreeImporter(GenericTangosTool):
    tool_name = 'import-ahf-trees'
    tool_description = "Import merger trees calculated by AHF's merger tree tools."
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')


    def process_options(self, options):
        self.options = options


    @staticmethod
    def filename_to_snapnum(filename):
        match = re.match(".*snapdir_([0-9]{3})/?", filename)
        if match:
            return int(match.group(1))
        else:
            raise ValueError("Unable to convert %s to snapshot number"%filename)

    def create_timestep_halo_dictionary(self, ts):
        halos = ts.halos.all()
        out = {}
        for h in halos:
            out[h.finder_id] = h
        return out

    def create_links(self, ts, ts_next, link_dictionary):
        session = db.get_default_session()
        d_id = get_or_create_dictionary_item(session, "ahf_tree_link")
        objs_this = self.create_timestep_halo_dictionary(ts)
        objs_next = self.create_timestep_halo_dictionary(ts_next)
        links = []
        for this_id, (next_id, merger_ratio) in link_dictionary:
            this_obj = objs_this.get(this_id, None)
            next_obj = objs_next.get(next_id, None)
            if this_obj is not None and next_obj is not None:
                links.append(HaloLink(this_obj, next_obj, d_id, merger_ratio))
                links.append(HaloLink(next_obj, this_obj, d_id, merger_ratio))
        session.add_all(links)
        session.commit()
        logger.info("%d links created between %s and %s",len(links), ts, ts_next)


    def run_calculation_loop(self):
        simulations = db.sim_query_from_name_list(self.options.sims)

        for simulation in simulations:
            logger.info("Processing %s",simulation)
            for ts in simulation.timesteps:
                ts_prev = ts.previous 
                # ahf merger tree tool goes back in time 
                if ts_prev is not None:
                    #additionally check if this is the first snapshot
                    tree = at.AHFTree(os.path.join(config.base,simulation.basename), ts)
                    self.create_links(ts_prev, ts, tree.get_links_for_snapshot())

