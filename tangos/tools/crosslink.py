from __future__ import absolute_import
import argparse

import tangos as db
import tangos.core, tangos.parallel_tasks.database
from .. import config
from tangos import parallel_tasks
from tangos import core
import sqlalchemy, sqlalchemy.orm
from tangos.log import logger
import numpy as np
from six.moves import zip
from . import GenericTangosTool

class GenericLinker(GenericTangosTool):
    def __init__(self, session=None):
        self.session = session or core.get_default_session()

    def process_options(self, options):
        self.args = options

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument("--verbose", action="store_true",
                            help="Print extra information")
        parser.add_argument("--force", action="store_true",
                            help="Generate links even if they already exist for those timesteps")
        parser.add_argument('--type', action='store', type=str, dest='type_', default='halo',
                            help="Secify the object type to run on by tag name (or integer). "
                                 "Can be halo (default), group, or BH.")
        parser.add_argument("--hmax", action="store", type=int, default=None,
                            help="Specify the maximum number of halos per snapshot")
        parser.add_argument('--backwards', action='store_true',
                            help='Process in reverse order (low-z first)')
        parser.add_argument('--dmonly', action='store_true',
                            help='only match halos based on DM particles. Much more memory efficient, but currently only works for Rockstar halos')

    def run_calculation_loop(self):
        parallel_tasks.database.synchronize_creator_object()
        pair_list = self._generate_timestep_pairs()

        if len(pair_list)==0:
            logger.error("No timesteps found to link")
            return

        pair_list = parallel_tasks.distributed(pair_list)

        object_type = core.halo.Halo.object_typecode_from_tag(self.args.type_)

        for s_x, s in pair_list:
            logger.info("Linking %r and %r",s_x,s)
            if self.args.force or self.need_crosslink_ts(s_x, s, object_type):
                self.crosslink_ts(s_x, s, 0, self.args.hmax, self.args.dmonly, object_typecode=object_type)

    def _generate_timestep_pairs(self):
        raise NotImplementedError("No implementation found for generating the timestep pairs")

    def get_halo_entry(self, ts, halo_number):
        h = ts.halos.filter_by(finder_id=halo_number).first()
        return h

    def need_crosslink_ts(self, ts1, ts2, object_typecode=0):
        num_sources = ts1.halos.count()
        num_targets = ts2.halos.count()
        if num_targets == 0:
            logger.warn("Will not link: no halos in target timestep %r", ts2)
            return False
        if num_sources == 0:
            logger.warn("Will not link: no halos in source timestep %r", ts1)
            return False

        halo_source = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_source")
        halo_target = sqlalchemy.orm.aliased(core.halo.Halo, name="halo_target")
        same_d_id = core.dictionary.get_or_create_dictionary_item(self.session, "ptcls_in_common").id
        exists = self.session.query(core.halo_data.HaloLink).join(halo_source, core.halo_data.HaloLink.halo_from). \
                    join(halo_target, core.halo_data.HaloLink.halo_to). \
                    filter(halo_source.timestep_id == ts1.id, halo_target.timestep_id == ts2.id,
                           halo_source.object_typecode == object_typecode,
                           halo_target.object_typecode == object_typecode,
                        core.halo_data.HaloLink.relation_id == same_d_id).count() > 0
        self.session.commit()

        if exists:
            logger.warn("Will not link: links already exist between %r and %r", ts1, ts2)
            return False
        return True

    def create_db_objects_from_catalog(self, cat, finder_id_to_halos_1, finder_id_to_halos_2, same_d_id):
        items = []
        missing_db_object = 0
        for i, possibilities in enumerate(cat):
            h1 = finder_id_to_halos_1.get(i, None)
            for cat_i, weight in possibilities:
                h2 = finder_id_to_halos_2.get(cat_i, None)

                if h1 is not None and h2 is not None:
                    items.append(core.halo_data.HaloLink(h1, h2, same_d_id, weight))
                else:
                    missing_db_object += 1

        if missing_db_object > 0:
            logger.warn("%d link(s) could not be identified because the halo objects do not exist in the DB",
                        missing_db_object)
        return items

    def make_finder_id_to_halo_map(self, ts, object_typecode):
        halos = ts.objects.filter_by(object_typecode=object_typecode).all()
        halos_map = {h.finder_id: h for h in halos}
        return halos_map

    def crosslink_ts(self, ts1, ts2, halo_min=0, halo_max=None, dmonly=False, threshold=config.default_linking_threshold, object_typecode=0):
        """Link the halos of two timesteps together

        :type ts1 tangos.core.TimeStep
        :type ts2 tangos.core.TimeStep"""
        logger.info("Gathering halo information for %r and %r", ts1, ts2)
        halos1 = self.make_finder_id_to_halo_map(ts1, object_typecode)
        halos2 = self.make_finder_id_to_halo_map(ts2, object_typecode)

        with parallel_tasks.ExclusiveLock("create_db_objects_from_catalog"):
            same_d_id = core.dictionary.get_or_create_dictionary_item(self.session, "ptcls_in_common")
            self.session.commit()

        output_handler_1 = ts1.simulation.get_output_handler()
        output_handler_2 = ts2.simulation.get_output_handler()
        if type(output_handler_1).match_objects != type(output_handler_2).match_objects:
            logger.error("Timesteps %r and %r cannot be crosslinked; they are using incompatible file readers",
                         ts1, ts2)
            return

        # keep the files alive throughout (so they are not garbage-collected after the first match_objects):
        snap1 = ts1.load()
        snap2 = ts2.load()

        try:
            cat = output_handler_1.match_objects(ts1.extension, ts2.extension, halo_min, halo_max, dmonly, threshold,
                                                 core.halo.Halo.object_typetag_from_code(object_typecode),
                                                 output_handler_for_ts2=output_handler_2)
            back_cat = output_handler_2.match_objects(ts2.extension, ts1.extension, halo_min, halo_max, dmonly, threshold,
                                                      core.halo.Halo.object_typetag_from_code(object_typecode),
                                                      output_handler_for_ts2= output_handler_1)
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            logger.exception("Exception during attempt to crosslink timesteps %r and %r", ts1, ts2)
            return

        with self.session.no_autoflush:
            logger.info("Gathering links for %r and %r", ts1, ts2)
            items = self.create_db_objects_from_catalog(cat, halos1, halos2, same_d_id)
            logger.info("Identified %d links between %r and %r", len(items), ts1, ts2)
            items_back = self.create_db_objects_from_catalog(back_cat, halos2, halos1, same_d_id)
            logger.info("Identified %d links between %r and %r", len(items_back), ts2, ts1)

        with parallel_tasks.ExclusiveLock("create_db_objects_from_catalog"):
            logger.info("Preparing to commit links for %r and %r", ts1, ts2)
            self.session.add_all(items)
            self.session.add_all(items_back)
            self.session.commit()
        logger.info("Finished committing total of %d links for %r and %r", len(items)+len(items_back), ts1, ts2)

class TimeLinker(GenericLinker):
    tool_name = "link"
    tool_description = "Generate merger tree and other information linking tangos objects over time"

    def _generate_timestep_pairs(self):
        logger.info("generating pairs of timesteps")
        base_sim = db.sim_query_from_name_list(self.args.sims)
        pairs = []
        for x in base_sim:
            ts = self.session.query(tangos.core.timestep.TimeStep).filter_by(
                simulation_id=x.id, available=True).order_by(tangos.core.timestep.TimeStep.redshift.desc()).all()
            for a, b in zip(ts[:-1], ts[1:]):
                pairs.append((a, b))
        if self.args.backwards:
            pairs = pairs[::-1]
        return pairs

    @classmethod
    def add_parser_arguments(self, parser):
        super(TimeLinker, self).add_parser_arguments(parser)
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')


class CrossLinker(GenericLinker):
    tool_name = "crosslink"
    tool_description = "Identify the same objects between two simulations and link them"

    @classmethod
    def add_parser_arguments(self, parser):
        super(CrossLinker, self).add_parser_arguments(parser)
        parser.add_argument('sims', action='store', nargs=2,
                            metavar=('name1', 'name2'),
                            help='The two simulations (or, optionally, two individual timesteps) to crosslink')

    def _generate_timestep_pairs(self):
        obj1 = db.get_item(self.args.sims[0])
        obj2 = db.get_item(self.args.sims[1])
        if isinstance(obj1, core.TimeStep) and isinstance(obj2, core.TimeStep):
            return [[obj1, obj2]]
        elif isinstance(obj1, core.Simulation) and isinstance(obj2, core.Simulation):
            return self._generate_timestep_pairs_from_sims(obj1, obj2)
        else:
            raise ValueError("No way to link %r to %r"%(obj1, obj2))

    def _generate_timestep_pairs_from_sims(self, sim1, sim2):
        assert sim1 != sim2, "Can't link simulation to itself"

        logger.info("Match timesteps of %r to %r",sim1,sim2)

        ts1s = sim1.timesteps
        ts2s = sim2.timesteps

        pairs = []
        for ts1 in ts1s:
            ts2 = self._get_best_timestep_matching(ts2s, ts1)
            pairing_is_mutual = (self._get_best_timestep_matching(ts1s,ts2)==ts1)
            if pairing_is_mutual:
                logger.info("Pairing timesteps: %r and %r", ts1, ts2)
                pairs+=[(ts1,ts2)]
            else:
                logger.warn("No pairing found for timestep %r",ts1)

        return pairs

    def _get_best_timestep_matching(self, list_of_candidates, timestep_to_match):
        return min(list_of_candidates, key=lambda ts2: abs(ts2.time_gyr - timestep_to_match.time_gyr))
