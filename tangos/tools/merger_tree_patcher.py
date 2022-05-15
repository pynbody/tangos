import argparse

import numpy as np
from sqlalchemy import and_, or_, orm

from .. import core, query
from ..core import get_or_create_dictionary_item
from ..log import logger
from . import GenericTangosTool


class MergerTreePruner(GenericTangosTool):
    tool_name = 'prune-trees'
    tool_description = '[Experimental] Remove questionable links from merger trees'
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')

        parser.add_argument('--min-weight-progenitor', action='store', type=float, default=0,
                            help='The minimum weight that should be associated with a progenitor for retention. '
                                 'To retain minor progenitors, this should normally be set to zero.')

        parser.add_argument('--min-weight-descendant', action='store', type=float, default=0.05,
                            help='The minimum weight that should be associated with a descendant for retention. '
                                 'Weights close to zero are normally suspect, since they indicate the halo'
                                 'transfers only a small fraction of its mass into the descendant.')
        # TO IMPLEMENT:
        parser.add_argument('--max-NDM-progenitor', action='store', type=float, default=1.0,
                            help='The maximum relative increase in number of dark matter particles in the progenitor'
                                 'for retention. For example, if this is set to 1.0 (default), the link is dropped'
                                 'if there are more than twice as many DM particles in the progenitor as in the'
                                 'descendant, since this suggests a mis-identification.')


    def process_options(self, options: argparse.ArgumentParser):
        self.options = options

    def process_timestep(self, ts: core.TimeStep):
        session = core.get_default_session()

        next_ts = ts.next
        if next_ts is None:
            return


        progenitor_from_alias = orm.aliased(core.halo.SimulationObjectBase)
        progenitor_to_alias = orm.aliased(core.halo.SimulationObjectBase)
        progenitor_halolink = orm.aliased(core.HaloLink)
        descendant_halolink = orm.aliased(core.HaloLink)

        progenitor_links = session.query(progenitor_halolink.id, descendant_halolink.id)\
            .join(progenitor_from_alias, and_(progenitor_halolink.halo_from_id == progenitor_from_alias.id, progenitor_from_alias.timestep_id == ts.id))\
            .join(progenitor_to_alias, and_(progenitor_halolink.halo_to_id == progenitor_to_alias.id, progenitor_to_alias.timestep_id == next_ts.id))\
            .join(descendant_halolink, and_(progenitor_halolink.halo_to_id == descendant_halolink.halo_from_id,
                                            progenitor_halolink.halo_from_id == descendant_halolink.halo_to_id))\
            .filter(or_(progenitor_halolink.weight < self.options.min_weight_progenitor,
                        descendant_halolink.weight < self.options.min_weight_descendant,
                        progenitor_to_alias.NDM > (1.0 + self.options.max_NDM_progenitor) * progenitor_from_alias.NDM))

        delete_links = []
        progenitors_and_descendants = progenitor_links.all()
        for p,d in progenitors_and_descendants:
            delete_links+=[p,d]

        row_count = session.query(core.HaloLink).filter(core.HaloLink.id.in_(delete_links)).delete()
        session.commit()
        if row_count>0:
            logger.info(f"Deleted {row_count} links between timesteps {ts} and {next_ts}")

    def run_calculation_loop(self):
        simulations = core.sim_query_from_name_list(self.options.sims)
        logger.info("This tool is experimental. Use at your own risk!")
        for simulation in simulations:
            logger.info("Processing %s", simulation)

            for ts in simulation.timesteps[::-1]:
                 self.process_timestep(ts)


class MergerTreePatcher(GenericTangosTool):
    tool_name = 'patch-trees'
    tool_description = "[Experimental] Attempt to patch up merger trees"
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


    def create_link(self, halo, matched):
        session = core.get_default_session()
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
                    match = query.get_halo(match_dbid)
                    self.create_link(halo,match)
                    success = True
                    break

            ts_candidate = ts_candidate.previous

        if not success:
            logger.info(f"No luck in finding a match for {halo}")


    def run_calculation_loop(self):
        simulations = core.sim_query_from_name_list(self.options.sims)
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
                    obj = query.get_halo(dbid)
                    self.fixup(obj)
