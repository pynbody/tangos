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

        parser.add_argument('--max-NDM-progenitor', action='store', type=float, default=2.0,
                            help='The maximum relative increase in number of dark matter particles in the progenitor'
                                 'for retention. For example, if this is set to 2.0 (default), the link is dropped'
                                 'if there are more than twice as many DM particles in the progenitor as in the'
                                 'descendant, since this suggests a mis-identification.')

        parser.add_argument('--min-NDM-progenitor', action='store', type=float, default=0.33,
                            help='The maximum relative  number of dark matter particles in the progenitor'
                                 'for retention. For example, if this is set to 0.33 (default), the link is dropped'
                                 'if there are less than a third as many DM particles in the progenitor as in the'
                                 'descendant, since this suggests a mis-identification.')


    def process_options(self, options: argparse.ArgumentParser):
        self.options = options

    def process_timestep(self, early_timestep: core.TimeStep):
        session = core.get_default_session()

        late_timestep = early_timestep.next
        if late_timestep is None:
            return


        descendant_alias = orm.aliased(core.halo.SimulationObjectBase, name="descendant")
        progenitor_alias = orm.aliased(core.halo.SimulationObjectBase, name="progenitor")
        progenitor_halolink = orm.aliased(core.HaloLink, name="descendant_to_progenitor_link")
        descendant_halolink = orm.aliased(core.HaloLink, name="progenitor_to_descendant_link")

        progenitor_links = session.query(progenitor_halolink.id, descendant_halolink.id)\
            .join(descendant_alias, and_(progenitor_halolink.halo_from_id == descendant_alias.id, descendant_alias.timestep_id == late_timestep.id))\
            .join(progenitor_alias, and_(progenitor_halolink.halo_to_id == progenitor_alias.id, progenitor_alias.timestep_id == early_timestep.id))\
            .outerjoin(descendant_halolink, and_(progenitor_halolink.halo_to_id == descendant_halolink.halo_from_id,
                                            progenitor_halolink.halo_from_id == descendant_halolink.halo_to_id))\
            .filter(or_(progenitor_halolink.weight < self.options.min_weight_progenitor,
                        descendant_halolink.weight < self.options.min_weight_descendant,
                        progenitor_alias.NDM > self.options.max_NDM_progenitor * descendant_alias.NDM,
                        progenitor_alias.NDM < self.options.min_NDM_progenitor * descendant_alias.NDM))


        delete_links = []
        progenitors_and_descendants = progenitor_links.all()
        for p,d in progenitors_and_descendants:
            delete_links.append(p)
            if d is not None:
                delete_links.append(d)

        row_count = session.query(core.HaloLink).filter(core.HaloLink.id.in_(delete_links)).delete()
        session.commit()
        if row_count>0:
            logger.info(f"Deleted {row_count} links between timesteps {early_timestep} and {late_timestep}")

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
        parser.add_argument('--max-for-match', action='store', type=float, default=0.1,
                            help='The maximum fractional offset in values of variables specified in relative-match'
                                 'that qualifies as a match')
        parser.add_argument('--position-variable', action='store', type=str, default='shrink_center/a()',
                            help='The name of the variable which represents the 3d position')
        parser.add_argument('--max-position-offset', action='store', type=float, default=100.0,
                            help='The maximum value of the position offset that is allowed in comoving units (i.e.'
                                 'will be divided by scalefactor)')
        parser.add_argument('--max-timesteps', action='store', type=int, default=6,
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

    _candidates_cache = {}

    def _get_candidate_id_position_values(self, ts_candidate):
        if ts_candidate.id not in self._candidates_cache:
            self._candidates_cache[ts_candidate.id] = ts_candidate.calculate_all("dbid()",
                                                                      self.options.position_variable,
                                                                      *self.options.relative_match)
        return self._candidates_cache[ts_candidate.id]

    def fixup(self, halo):
        success = False
        ts_candidate = halo.timestep.previous
        if ts_candidate is None:
            return

        source_values = [halo.calculate(m) for m in self.options.relative_match]
        source_position = halo.calculate(self.options.position_variable)

        for i in range(self.options.max_timesteps):

            dbid, candidate_positions, *candidate_values = self._get_candidate_id_position_values(ts_candidate)

            offsets = [cv-sv for sv, cv in zip(source_values, candidate_values)]
            rel_offsets = [(o/sv)**2 for o, sv in zip(offsets, source_values)]
            rel_offsets = np.sqrt(sum(rel_offsets))

            pos_offset = np.linalg.norm(source_position - candidate_positions, axis=1)
            mask = pos_offset < self.options.max_position_offset*(1.+ts_candidate.redshift)

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
            if ts_candidate is None:
                break

        if not success:
            logger.info(f"No luck in finding a match for {halo}")


    def run_calculation_loop(self):
        simulations = core.sim_query_from_name_list(self.options.sims)
        logger.info("This tool is experimental. Use at your own risk!")

        for simulation in simulations:
            logger.info("Processing %s",simulation)
            logger.info(self.options.include_only)
            include_criterion = "(!has_link(earlier(1))) & " + (" & ".join(self.options.include_only))

            logger.info("Query for missing links is %s",include_criterion)
            for ts in simulation.timesteps[::-1]:
                dbids, flag = ts.calculate_all("dbid()", include_criterion)
                dbids = dbids[flag]
                logger.info(f"Timestep {ts} has {len(dbids)} broken links to consider")
                for dbid in dbids:
                    obj = query.get_halo(dbid)
                    self.fixup(obj)
