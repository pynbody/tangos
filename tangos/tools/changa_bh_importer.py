import glob

import numpy as np

from .. import config, core, parallel_tasks, tracking
from ..input_handlers.changa_bh import BlackHolesLog, ShortenedOrbitLog
from ..log import logger
from ..util.check_deleted import check_deleted
from . import GenericTangosTool


class ChangaBHImporter(GenericTangosTool):
    tool_name = 'import-changa-bh'
    tool_description = 'Import black holes and link them to parent halos'

    @classmethod
    def add_parser_arguments(cls, parser):
        parser.add_argument('--sims', '--for', action='store', nargs=1,
                            metavar='simulation_name',
                            help='Specify a simulation to run on')
        parser.add_argument('--link-only', action='store_true',
                            help='Do not look for any new black holes; only link already-known black holes to new timesteps or halos')
        parser.add_argument('--backwards', action='store_true',
                            help="Scan through timesteps from low to high redshift (default is high to low)")

    def process_options(self, options):
        self.args = options

    def run_calculation_loop(self):
        parallel_tasks.database.synchronize_creator_object()
        self._session = core.get_default_session()

        if not self.args.link_only:
            self._import_black_holes()

        self._timelink_black_holes()

    def _get_timesteps(self):
        sim_query = core.sim_query_from_name_list(self.args.sims, self._session)
        timesteps = core.get_default_session().query(core.timestep.TimeStep).filter(
            core.timestep.TimeStep.simulation_id.in_([q.id for q in sim_query.all()])). \
            order_by(core.timestep.TimeStep.time_gyr).all()
        if self.args.backwards:
            timesteps = timesteps[::-1]
        return timesteps

    def _timelink_black_holes(self):
        query = core.sim_query_from_name_list(self.args.sims, self._session)
        for sim in query.all():
            pairs = list(zip(sim.timesteps[:-1], sim.timesteps[1:]))
            candidate_filenames = glob.glob(config.base + "/" + sim.basename + "/*.BHmergers")
            if len(candidate_filenames) == 0:
                logger.error("No merger file for " + sim.basename)
                return
            elif len(candidate_filenames) > 1:
                logger.error("Can't work out which is the merger file for " + sim.basename)
                logger.error("Found: %s", candidate_filenames)
                return
            self._bhmerger_filename = candidate_filenames[0]
            with self._session.no_autoflush:
                self._generate_halolinks(pairs)

    def _generate_halolinks(self, pairs):
        for ts1, ts2 in parallel_tasks.distributed(pairs):
            if BlackHolesLog.can_load(ts2.filename):
                bh_log = BlackHolesLog(ts2.filename)
            elif ShortenedOrbitLog.can_load(ts2.filename):
                bh_log = ShortenedOrbitLog(ts2.filename)
            else:
                logger.error("Warning! No orbit file found!")

            links = []
            mergers_links = []
            bh_map = {}
            logger.info("Gathering BH tracking information for steps %r and %r", ts1, ts2)
            with parallel_tasks.ExclusiveLock("bh"):
                dict_obj = core.get_or_create_dictionary_item(self._session, "tracker")
                dict_obj_next = core.get_or_create_dictionary_item(self._session, "BH_merger_next")
                dict_obj_prev = core.get_or_create_dictionary_item(self._session, "BH_merger_prev")

            track_links_n, idf_n, idt_n = tracking.get_tracker_links(self._session, dict_obj_next)
            bh_objects_1, nums1, id1 = self._get_bh_objs_numbers_and_dbids(ts1)
            bh_objects_2, nums2, id2 = self._get_bh_objs_numbers_and_dbids(ts2)
            tracker_links, idf, idt = tracking.get_tracker_links(self._session, dict_obj)

            idf_n = np.array(idf_n)
            idt_n = np.array(idt_n)

            if len(nums1) == 0 or len(nums2) == 0:
                logger.info("No BHs found in either step %r or %r... moving on", ts1, ts2)
                continue

            logger.info("Generating BH tracker links between steps %r and %r", ts1, ts2)
            o1 = np.where(np.in1d(nums1, nums2))[0]
            o2 = np.where(np.in1d(nums2, nums1))[0]
            if len(o1) == 0 or len(o2) == 0:
                continue
            with self._session.no_autoflush:
                for ii, jj in zip(o1, o2):
                    if nums1[ii] != nums2[jj]:
                        raise RuntimeError("BH iords are mismatched")
                    exists = np.where((idf == id1[ii]) & (idt == id2[jj]))[0]
                    if len(exists) == 0:
                        links.append(core.halo_data.HaloLink(bh_objects_1[ii], bh_objects_2[jj], dict_obj, 1.0))
                        links.append(core.halo_data.HaloLink(bh_objects_2[jj], bh_objects_1[ii], dict_obj, 1.0))
            logger.info("Generated %d tracker links between steps %r and %r", len(links), ts1, ts2)

            logger.info("Generating BH Merger information for steps %r and %r", ts1, ts2)
            for l in open(self._bhmerger_filename):
                l_split = l.split()
                t = float(l_split[6])
                bh_dest_id = int(l_split[0])
                bh_src_id = int(l_split[1])
                ratio = float(l_split[4])

                # ratios in merger file are ambiguous (since major progenitor may be "source" rather than "destination")
                # re-establish using the log file:
                try:
                    ratio = bh_log.determine_merger_ratio(bh_src_id, bh_dest_id)
                except (ValueError, AttributeError) as e:
                    logger.debug(
                        "Could not calculate merger ratio for %d->%d from the BH log; assuming the .BHmergers-asserted value is accurate",
                        bh_src_id, bh_dest_id)

                if t > ts1.time_gyr and t <= ts2.time_gyr:
                    bh_map[bh_src_id] = (bh_dest_id, ratio)

            self._resolve_multiple_mergers(bh_map)
            logger.info("Gathering BH merger links for steps %r and %r", ts1, ts2)
            with self._session.no_autoflush:
                for src, (dest, ratio) in bh_map.items():
                    if src not in nums1 or dest not in nums2:
                        logger.warning("Can't link BH %r -> %r; missing BH objects in database", src, dest)
                        continue
                    bh_src_before = bh_objects_1[nums1.index(src)]
                    bh_dest_after = bh_objects_2[nums2.index(dest)]

                    if ((idf_n == bh_src_before.id) & (idt_n == bh_dest_after.id)).sum() == 0:
                        mergers_links.append(
                            core.halo_data.HaloLink(bh_src_before, bh_dest_after, dict_obj_next, 1.0))
                        mergers_links.append(
                            core.halo_data.HaloLink(bh_dest_after, bh_src_before, dict_obj_prev, ratio))

            logger.info("Generated %d BH merger links for steps %r and %r", len(mergers_links), ts1, ts2)

            with parallel_tasks.ExclusiveLock("bh"):
                logger.info("Committing total %d BH links for steps %r and %r", len(mergers_links) + len(links), ts1,
                            ts2)
                self._session.add_all(links)
                self._session.add_all(mergers_links)
                self._session.commit()
                logger.info("Finished committing BH links for steps %r and %r", ts1, ts2)

    def _generate_missing_bh_objects(self, bh_iord, f, existing_obj_num):
        halo = []
        for bhi in bh_iord:
            if bhi not in existing_obj_num:
                halo.append(core.halo.BH(f, int(bhi)))

        return halo

    def _collect_bh_trackers(self, bh_iord, sim, existing_trackers):
        track = []
        for bhi in bh_iord:
            bhi = int(bhi)
            et = np.where(existing_trackers == bhi)[0]
            if len(et) == 0:
                tx = core.tracking.TrackData(sim, bhi)
                tx.particles = [bhi]
                tx.use_iord = True
                track.append(tx)
        return track

    def _get_bh_halo_assignments(self, pynbody_snapshot):
        pynbody_halos = pynbody_snapshot.halos()
        bh_cen_halos = None
        bh_halos = None
        import pynbody

        if isinstance(pynbody_halos, pynbody.halo.RockstarIntermediateCatalogue):
            bh_cen_halos = pynbody_halos.get_group_array(family='BH')
        elif isinstance(pynbody_halos, pynbody.halo.AHFCatalogue):
            bh_cen_halos = pynbody_halos.get_group_array(top_level=False, family='bh')
            bh_halos = pynbody_halos.get_group_array(top_level=True, family='bh')
        else:
            pynbody_snapshot['gp'] = pynbody_halos.get_group_array()
            bh_cen_halos = pynbody_snapshot.star['gp'][np.where(pynbody_snapshot.star['tform'] < 0)[0]]

        with check_deleted(pynbody_halos):
            del pynbody_halos

        return bh_cen_halos, bh_halos

    def _import_black_holes(self):
        for timestep in parallel_tasks.distributed(self._get_timesteps()):
            logger.info("Processing %s", timestep)

            try:
                timestep_particle_data = timestep.load()
            except:
                logger.warning("File not found - continuing")
                continue

            if len(timestep_particle_data.star) < 1:
                logger.warning("No stars - continuing")
                continue

            timestep_particle_data.physical_units()

            logger.info("Gathering existing BH halo information from database for step %r", timestep)

            bhobjs = timestep.bhs.all()
            existing_bh_nums = [x.halo_number for x in bhobjs]

            logger.info("...found %d existing BHs", len(existing_bh_nums))

            logger.info("Gathering BH info from simulation for step %r", timestep)
            bh_iord_this_timestep = timestep_particle_data.star['iord'][
                np.where(timestep_particle_data.star['tform'] < 0)[0]]
            bh_mass_this_timestep = timestep_particle_data.star['mass'][
                np.where(timestep_particle_data.star['tform'] < 0)[0]]

            logger.info("Found %d black holes for %r", len(bh_iord_this_timestep), timestep)

            logger.info("Updating BH trackdata and BH objects using on-disk information from %r", timestep)

            self._add_missing_trackdata_and_BH_objects(timestep, bh_iord_this_timestep, existing_bh_nums)
            self._session.expire_all()

            logger.info("Calculating halo associations for BHs in timestep %r", timestep)
            bh_cen_halos, bh_halos = self._get_bh_halo_assignments(timestep_particle_data)

            # re-order our information so that links refer to BHs in descending order of mass
            bh_order_by_mass = np.argsort(bh_mass_this_timestep)[::-1]
            bh_iord_this_timestep = bh_iord_this_timestep[bh_order_by_mass]
            if bh_halos is not None:
                bh_halos = bh_halos[bh_order_by_mass]
            if bh_cen_halos is not None:
                bh_cen_halos = bh_cen_halos[bh_order_by_mass]

            logger.info("Freeing the timestep particle data")
            with check_deleted(timestep_particle_data):
                del (timestep_particle_data)

            if bh_halos is not None:
                self._assign_bh_to_halos(bh_halos, bh_iord_this_timestep, timestep, "BH")
            if bh_cen_halos is not None:
                self._assign_bh_to_halos(bh_cen_halos, bh_iord_this_timestep, timestep, "BH_central", "host_halo")

    def _assign_bh_to_halos(self, bh_halo_assignment, bh_iord, timestep, linkname, hostname=None):
        linkname_dict_id = core.dictionary.get_or_create_dictionary_item(self._session, linkname)
        if hostname is not None:
            host_dict_id = core.dictionary.get_or_create_dictionary_item(self._session, hostname)
        else:
            host_dict_id = None

        logger.info("Gathering %s links for step %r", linkname, timestep)

        links, link_id_from, link_id_to = tracking.get_tracker_links(self._session, linkname_dict_id)
        halos = timestep.halos.filter_by(object_typecode=0).all()

        halo_nums = [h.halo_number for h in halos]
        halo_catind = [h.finder_offset for h in halos]
        halo_ids = np.array([h.id for h in halos])

        logger.info("Gathering bh halo information for %r", timestep)
        with parallel_tasks.lock.SharedLock("bh"):
            bh_database_object, existing_bh_nums, bhobj_ids = self._get_bh_objs_numbers_and_dbids(timestep)

        bh_links = []

        with self._session.no_autoflush:
            for bhi, haloi in zip(bh_iord, bh_halo_assignment):
                haloi = int(haloi)
                bhi = int(bhi)
                if haloi not in halo_catind:
                    logger.warning("Skipping BH in halo %d as no corresponding halo found in the database", haloi)
                    continue
                if bhi not in existing_bh_nums:
                    logger.warning("Can't find the database object for BH %d", bhi)
                    print(bhi)
                    print(existing_bh_nums)
                    continue

                bh_index_in_list = existing_bh_nums.index(bhi)
                halo_index_in_list = halo_catind.index(haloi)
                bh_obj = bh_database_object[bh_index_in_list]
                halo_obj = halos[halo_index_in_list]

                num_existing_links = ((link_id_from == halo_ids[halo_index_in_list]) & (
                            link_id_to == bhobj_ids[bh_index_in_list])).sum()
                if num_existing_links == 0:
                    bh_links.append(core.halo_data.HaloLink(halo_obj, bh_obj, linkname_dict_id))
                    if host_dict_id is not None:
                        bh_links.append(core.halo_data.HaloLink(bh_obj, halo_obj, host_dict_id))

        logger.info("Committing %d %s links for step %r...", len(bh_links), linkname, timestep)
        with parallel_tasks.ExclusiveLock("bh"):
            self._session.add_all(bh_links)
            self._session.commit()
        logger.info("...done")

    def _get_bh_objs_numbers_and_dbids(self, timestep):
        bh_database_object = timestep.bhs.all()
        existing_bh_nums = [x.halo_number for x in bh_database_object]
        bhobj_ids = np.array([x.id for x in bh_database_object])
        return bh_database_object, existing_bh_nums, bhobj_ids

    def _add_missing_trackdata_and_BH_objects(self, timestep, this_step_bh_iords, existing_bhobj_iords):
        with parallel_tasks.ExclusiveLock("bh"):
            track, track_nums = tracking.get_trackers(timestep.simulation)
            with self._session.no_autoflush:
                tracker_to_add = self._collect_bh_trackers(this_step_bh_iords, timestep.simulation, track_nums)
                halo_to_add = self._generate_missing_bh_objects(this_step_bh_iords, timestep, existing_bhobj_iords)

            self._session.add_all(tracker_to_add)
            self._session.add_all(halo_to_add)
            self._session.commit()
        logger.info("Committed %d new trackdata and %d new BH objects for %r", len(tracker_to_add),
                    len(halo_to_add), timestep)

    def _resolve_multiple_mergers(self, bh_map):
        for k, v in bh_map.items():
            if v[0] in bh_map:
                old_target = v[0]
                old_weight = v[1]
                bh_map[k] = bh_map[old_target][0], v[1] * bh_map[old_target][1]
                logger.info("Multi-merger detected; reassigning %d->%d (old) %d (new)", k, old_target, bh_map[k][0])
                self._resolve_multiple_mergers(bh_map)
                return
