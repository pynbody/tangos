import re

from .. import config, core
from ..core import get_or_create_dictionary_item
from ..core.halo_data import HaloLink, HaloProperty
from ..input_handlers import pynbody
from ..log import logger
from ..util import timestep_object_cache
from . import GenericTangosTool


class SubfindTreeImporter(GenericTangosTool):
    tool_name = 'import-subfind-trees'
    tool_description = "Import merger trees calculated on-the-fly by Gadget/Arepo"
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--sims', '--for', action='store', nargs='*',
                            metavar='simulation_name',
                            help='Specify a simulation (or multiple simulations) to run on')


    def process_options(self, options):
        self.options = options


    def create_links(self, ts, ts_next):
        """Create the descendant and progenitor links between two timesteps"""
        session = core.get_default_session()
        d_id = get_or_create_dictionary_item(session, "subfind_tree_link")

        obj_cache = timestep_object_cache.TimestepObjectCache(ts)
        obj_cache_next = timestep_object_cache.TimestepObjectCache(ts_next)

        links = self._create_links(ts, obj_cache_next, d_id, "Desc")
        links += self._create_links(ts_next, obj_cache, d_id, "Prog")
        session.add_all(links)
        session.commit()
        logger.info("%d links created between %s and %s",len(links), ts, ts_next)

    def _create_links(self, starting_timestep, object_cache_other_timestep, dictionary_item, subfind_link_type):
        """Follow the SubFind linked list of descendants or progenitors"""
        links = []
        for high_z_halo in starting_timestep.halos.all():

            this_properties = high_z_halo.load(mode='subfind-properties')
            link_to_finder_id = this_properties['First'+subfind_link_type+'SubhaloNr']
            if link_to_finder_id<0:
                # I have found cases where FirstProgSubhaloNr = -1, but there *is* actually a progenitor, given by
                # ProgSubhaloNr. Equally, ProgSubhaloNr sometimes points to a progenitor which is not the main
                # progenitor, so we can't just ignore FirstProgSubhaloNr. I am really baffled by what the intended
                # purpose of ProgSubhaloNr is (as opposed to FirstProgSubhaloNr). However this is a pragmatic fix,
                # using it as a fallback where FirstProgSubhaloNr is borked (again, unsure why that happens).
                link_to_finder_id = this_properties[subfind_link_type+'SubhaloNr']
            this_mass = this_properties['SubhaloMass']
            while link_to_finder_id >= 0:
                this_descendant_tangos_obj = object_cache_other_timestep.resolve_from_finder_id(link_to_finder_id, 'halo')
                if this_descendant_tangos_obj is None:
                    # The subfind halo wasn't imported into tangos. The link therefore can't be imported.
                    # In principle, we could continue the search for next descendants with a bit of hacking,
                    # but in practice it is presumably pointless because we won't have *even smaller* things
                    # than this in the tangos db
                    break
                properties_of_linked_subhalo = this_descendant_tangos_obj.load(mode='subfind-properties')

                ratio = min(properties_of_linked_subhalo['SubhaloMass'] / this_mass, 1.0)

                links.append(HaloLink(high_z_halo, this_descendant_tangos_obj, dictionary_item, ratio))

                # now move onto next descendant
                link_to_finder_id = properties_of_linked_subhalo['Next'+subfind_link_type+'SubhaloNr']
        return links

    @classmethod
    def _get_snap_id(self, filename):
        return int(re.match(r".*_(\d\d\d)(.hdf5)?", filename).group(1))

    def run_calculation_loop(self):
        simulations = core.sim_query_from_name_list(self.options.sims)

        for simulation in simulations:
            assert isinstance(simulation.get_output_handler(), pynbody.GadgetSubfindInputHandler), \
                "This tool can only import trees from Gadget+Subfind simulations"

            logger.info("Processing %s",simulation)
            timesteps = simulation.timesteps
            for ts_prev, ts in zip(timesteps[:-1], timesteps[1:]):
                if self._get_snap_id(ts.extension)!=self._get_snap_id(ts_prev.extension)+1:
                    logger.warning("The snapshots appear to skip steps. Attempt to import tree will continue, but may result in errors or an inconsistent merger history.")
                self.create_links(ts_prev, ts)
