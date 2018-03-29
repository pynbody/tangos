from __future__ import absolute_import
from __future__ import print_function

import tangos as db
import os
from ..input_handlers import consistent_trees as ct
from ..log import logger
from ..core import get_or_create_dictionary_item
from ..core.halo import PhantomHalo
from ..core.halo_data import HaloLink, HaloProperty
from .. import config
from . import GenericTangosTool
from six.moves import xrange
import re

class ConsistentTreesImporter(GenericTangosTool):
    tool_name = 'import-consistent-trees'
    tool_description = 'Import properties that were calculated by the halo finder'
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')

        parser.add_argument('--with-ids', action='store_true',
                            help="Store the consistent-trees ids as a property for each halo")


    def process_options(self, options):
        self.options = options


    @staticmethod
    def filename_to_snapnum(filename):
        match = re.match(".*(?:snapdir|snapshot)_([0-9]+)/?$", filename)
        if match:
            return int(match.group(1))
        else:
            raise ValueError("Unable to convert %s to snapshot number"%filename)

    def create_phantoms(self, timestep, n_phantoms):
        existing_phantoms = timestep.phantoms.all()
        existing_phantom_ids = []
        for p in existing_phantoms:
            existing_phantom_ids.append(p.finder_id)

        new_phantoms = []

        for i in xrange(1,n_phantoms+1):
            if -i not in existing_phantom_ids:
                new_ph = PhantomHalo(timestep, i, -i)
                new_phantoms.append(new_ph)

        session = db.core.get_default_session()

        session.add_all(new_phantoms)
        session.commit()
        logger.info("Add %d phantom halos to timestep %s", len(new_phantoms), timestep)
        logger.info("Total number of phantoms in tree %d; existing phantoms %d", n_phantoms, len(existing_phantoms))

    def create_timestep_halo_dictionary(self, ts):
        halos = ts.halos.all()
        phantoms = ts.phantoms.all()
        out = {}
        for h in halos:
            out[h.finder_id] = h
        for p in phantoms:
            out[p.finder_id] = p
        return out

    def create_links(self, ts, ts_next, link_dictionary):
        session = db.get_default_session()
        d_id = get_or_create_dictionary_item(session, "consistent_trees_link")
        objs_this = self.create_timestep_halo_dictionary(ts)
        objs_next = self.create_timestep_halo_dictionary(ts_next)
        links = []
        for this_id, (next_id, merger_ratio) in link_dictionary.items():
            this_obj = objs_this.get(this_id, None)
            next_obj = objs_next.get(next_id, None)
            if this_obj is not None and next_obj is not None:
                links.append(HaloLink(this_obj, next_obj, d_id, 1.0))
                links.append(HaloLink(next_obj, this_obj, d_id, merger_ratio))
        session.add_all(links)
        session.commit()
        logger.info("%d links created between %s and %s",len(links), ts, ts_next)

    def store_ids(self, ts, id_to_tree_id):
        session = db.get_default_session()
        objs = self.create_timestep_halo_dictionary(ts)
        dict_obj = get_or_create_dictionary_item(session, "consistent_trees_id")
        props = []
        for o in objs.values():
            tree_id = id_to_tree_id.get(o.finder_id, None)
            if tree_id is not None:
                props.append(HaloProperty(o, dict_obj, tree_id))
        session.add_all(props)
        session.commit()
        logger.info("%d consistent tree IDs added to step %s", len(props), ts)


    def run_calculation_loop(self):
        simulations = db.sim_query_from_name_list(self.options.sims)

        for simulation in simulations:
            logger.info("Processing %s",simulation)
            tree = ct.ConsistentTrees(os.path.join(config.base,simulation.basename))
            for ts in simulation.timesteps:
                snapnum = self.filename_to_snapnum(ts.extension)
                ts_next = ts.next

                if self.options.with_ids:
                    self.store_ids(ts, tree.get_finder_id_to_tree_id_for_snapshot(snapnum))

                if ts_next is not None:
                    n_phantoms = tree.get_num_phantoms_in_snapshot(snapnum+1)
                    self.create_phantoms(ts_next, n_phantoms)
                    self.create_links(ts, ts_next, tree.get_links_for_snapshot(snapnum))

