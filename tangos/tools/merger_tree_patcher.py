import os
import re

import tangos as db

from .. import config
from .. import core
from ..core import get_or_create_dictionary_item
from ..core.halo_data import HaloLink, HaloProperty
from ..input_handlers import ahf_trees as at
from ..log import logger
from . import GenericTangosTool
import numpy as np

class MergerTreePatcher(GenericTangosTool):
    tool_name = 'patch-trees'
    tool_description = "Attempt to patch up merger trees"
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')
        parser.add_argument('--include-only', action='store', type=str, nargs='*', default=['NDM()>100'],
                            help="Specify a filter that describes which objects the calculation should be executed for. "
                                 "Multiple filters may be specified, in which case they must all evaluate to true for "
                                 "the object to be included.")
        parser.add_argument('--relative-match', action='store',  type=str, default=['NStar()','NDM()'],
                            help='Variables to minimise when finding a halo match. If multiple expressions'
                                 'are provided, their relative differences are added in quadrature')
        parser.add_argument('--max-for-match', action='store', type=float, default=0.02,
                            help='The maximum fractional offset in values of variables specified in relative-match'
                                 'that qualifies as a match')
        parser.add_argument('--position-variable', action='store', type=str, default='shrink_center/a()',
                            help='The name of the variable which represents the 3d position')
        parser.add_argument('--max-position-offset', action='store', type=float, default=10.0,
                            help='The maximum value of the position offset that is allowed')
        parser.add_argument('--max-timesteps', action='store', type=int, default=3,
                            help='The maximum number of steps back in time to scan for a match')


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

    def create_link(self, halo, matched):
        session = db.get_default_session()
        core.creator.get_creator(session) # just to prevent any commits happening later when not expected

        d_id = get_or_create_dictionary_item(session, "patch-trees")
        logger.info(f"Creating a link from {halo} to {matched}")

        next = halo

        ts = halo.timestep.previous

        while ts!=matched.timestep:
            phantom = core.halo.PhantomHalo(ts, halo.halo_number, 0)
            # TODO: shouldn't assume this halo number is "free" for use, probably phantom halo IDs should be
            # allocated sequentially

            session.add(phantom)
            session.add(core.HaloLink(next, phantom, d_id, 1.0))
            session.add(core.HaloLink(phantom, next, d_id, 1.0))

            next = phantom
            ts = ts.previous

        session.add(core.HaloLink(next, matched, d_id, 1.0))
        session.add(core.HaloLink(matched, next, d_id, 1.0))
        session.commit()


    def fixup(self, halo):
        success = False
        ts_candidate = halo.timestep.previous
        source_values = [halo.calculate(m) for m in self.options.relative_match]
        source_position = halo.calculate(self.options.position_variable)

        for i in range(self.options.max_timesteps):

            dbid, candidate_positions, *candidate_values = ts_candidate.calculate_all("dbid()",
                                                                      self.options.position_variable,
                                                                      *self.options.relative_match)

            offsets = [cv-sv for sv, cv in zip(source_values, candidate_values)]
            rel_offsets = [(o/sv)**2 for o, sv in zip(offsets, source_values)]
            rel_offsets = np.sqrt(sum(rel_offsets))

            pos_offset = np.linalg.norm(source_position - candidate_positions, axis=1)
            mask = pos_offset < self.options.max_position_offset

            dbid = dbid[mask]
            rel_offsets = rel_offsets[mask]

            if len(rel_offsets)>0:
                if np.min(rel_offsets) < self.options.max_for_match:
                    match_dbid = dbid[np.argmin(rel_offsets)]
                    match = db.get_halo(match_dbid)
                    self.create_link(halo,match)
                    success = True
                    break

            ts_candidate = ts_candidate.previous

        if not success:
            logger.info(f"No luck in finding a match for {halo}")


    def run_calculation_loop(self):
        simulations = db.sim_query_from_name_list(self.options.sims)
        logger.info("This tool is experimental. Use at your own risk!")
        for simulation in simulations:
            logger.info("Processing %s",simulation)
            logger.info(self.options.include_only)
            include_criterion = "(!has_link(earlier(1))) & " + (" & ".join(self.options.include_only))
            # TODO: this is all very well when there actually isn't a link, but if a "bad" link is stored,
            # it's not much use (e.g. if a tiny progenitor is included but not the major progenitor, the
            # tool currently won't fix the major progenitor branch)

            logger.info("Query for missing links is %s",include_criterion)
            for ts in simulation.timesteps[::-1]:
                dbids, flag = ts.calculate_all("dbid()", include_criterion)
                dbids = dbids[flag]
                logger.info(f"Timestep {ts} has {len(dbids)} broken links to consider")
                for dbid in dbids:
                    obj = db.get_halo(dbid)
                    self.fixup(obj)


