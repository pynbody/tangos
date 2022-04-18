"""Support for the directory structure used by Eagle-like runs"""

from .pynbody import PynbodyInputHandler
import os
import re
import weakref
import scipy.stats
import numpy as np
from .. import config
from ..util import proxy_object
from ..log import logger
from six.moves import range

_eagle_underlying_subfind_cache = weakref.WeakValueDictionary()

class EagleLikeInputHandler(PynbodyInputHandler):
    patterns = ["snapshot_???_z*"]
    
    @classmethod
    def _snap_id_from_snapdir_path(cls, path):
        match = re.match(".*snapshot_([0-9]{3}_z[0-9]{3}p[0-9]{3})/?", path)
        if match:
            return match.group(1)
        else:
            return None

    @classmethod
    def _pynbody_extension_from_ts_extension(cls, path):
        snap_id = cls._snap_id_from_snapdir_path(path)
        if snap_id:
            return os.path.join(path, "snap_%s" % snap_id)
        else:
            raise IOError("Cannot infer correct path to pass to pynbody")

    @classmethod
    def _pynbody_subfind_extension_from_ts_extension(cls, path):
        snap_id = cls._snap_id_from_snapdir_path(path)
        if snap_id:
            subfind_path = os.path.join(os.path.dirname(path), "particledata_%s/eagle_subfind_particles_%s" % (snap_id, snap_id))
            if os.path.exists(os.path.dirname(subfind_path)):
                return subfind_path
            else:
                return cls._pynbody_extension_from_ts_extension(path)
        else:
            raise IOError("Cannot infer correct subfind particledata path to pass to pynbody")

    def _extension_to_filename(self, ts_extension):
        return str(os.path.join(config.base, self.basename, self._pynbody_extension_from_ts_extension(ts_extension)))

    def _extension_to_halodata_filename(self, ts_extension):
        return str(os.path.join(config.base, self.basename, self._pynbody_subfind_extension_from_ts_extension(ts_extension)))

    def _is_able_to_load(self, ts_extension):
        from .pynbody import pynbody
        filepath = self._extension_to_filename(ts_extension)
        try:
            pynbody.load(filepath)
        except (IOError, RuntimeError):
            return False

        halofilepath = self._extension_to_halodata_filename(ts_extension)
        try:
            pynbody.load(halofilepath)
        except (IOError, RuntimeError):
            return False

        return True

    def _construct_halo_cat(self, ts_extension, object_typetag):
        from .pynbody import pynbody
        if object_typetag!= 'halo' and object_typetag!='group':
            raise ValueError("Unknown object type %r" % object_typetag)

        f = self.load_timestep(ts_extension)

        halofilepath = self._extension_to_halodata_filename(ts_extension)
        to_unpack = getattr(f, "_eagle_subfind_cache", None)

        if to_unpack is None:
            logger.debug("Eagle input handler constructing GrpCatalogues")
            if halofilepath==self._extension_to_filename(ts_extension):
                f_subfind = f
            else:
                f_subfind = pynbody.load(halofilepath)
            h_group = f_subfind.halos()

            # Eagle files have group and subgroup numbers, but tangos can only associate a single "finder id" with
            # each object, so we need to map these pairs to a sensible unique subgroup
            self._create_unique_subgroup_ids(f_subfind)

            h_halo = pynbody.halo.GrpCatalogue(f_subfind, 'TangosSubGroupNumber',
                                          ignore=f_subfind['TangosSubGroupNumber'].max())
            logger.debug("Eagle input handler found on-disk total of %d groups and %d subgroups" % (len(h_group), len(h_halo)))
            h_halo.precalculate()
            h_group.precalculate()
            f._eagle_subfind_cache = f_subfind, h_group, h_halo
        else:
            f_subfind, h_group, h_halo = to_unpack

        if object_typetag=='halo':
            return h_halo
        elif object_typetag=='group':
            return h_group
        else:
            assert False # should have been caught above

    def _create_unique_subgroup_ids(self, f_subfind):
        # see if we previously saved it:
        if 'TangosSubGroupNumber' in f_subfind.loadable_keys():
            logger.info("Found cached TangosSubGroupNumber within Eagle snapshot")
            return

        grp = f_subfind['GroupNumber']
        subgrp = f_subfind['SubGroupNumber']
        logger.info("Mapping Eagle-like group/subgroups to unique subgroups across snapshot")
        # group together all particles that are not in a subgroup. The subgroup files do not have any particles
        # that are not in a group.
        unique_subgrp_hash = (subgrp.astype(np.int64) + (grp.astype(np.int64) << 32)).view(type=np.ndarray)
        subgrp_max = int(subgrp.max())
        unique_subgrp_hash[subgrp == subgrp_max] = subgrp_max
        unique_subgrp_ordered = scipy.stats.rankdata(unique_subgrp_hash, 'dense')
        unique_subgrp_ordered[subgrp == subgrp_max] = subgrp_max
        f_subfind['TangosSubGroupNumber'] = unique_subgrp_ordered
        try:
            f_subfind['TangosSubGroupNumber'].write()
        except IOError:
            logger.info("Unable to cache TangosSubGroupNumber on disk")
        return subgrp_max

        
    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        if object_typetag=='halo':
            return ['parent', 'original_subgroup_number']
        elif object_typetag=='group':
            return ['child']
        else:
            raise ValueError("Unknown object type %r"%object_typetag)

    @staticmethod
    def _second_largest(array):
        max_ = np.max(array)
        second_max = np.max(array[array!=max_])
        return second_max

    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        from .pynbody import pynbody
        if object_typetag not in ("halo","group"):
            raise ValueError("Unknown object type tag %r"%object_typetag)
        
        halofilepath = self._extension_to_halodata_filename(ts_extension)
        f_subfind = pynbody.load(halofilepath)
        self._create_unique_subgroup_ids(f_subfind)

        logger.info("Calculating child-parent relationships")
        children, unique_indices = np.unique(f_subfind['TangosSubGroupNumber'],return_index=True)
        parents = f_subfind['GroupNumber'][unique_indices]
        original = f_subfind['SubGroupNumber'][unique_indices]

        logger.info("Done; enumerating properties")
        if object_typetag=='halo':
            num_children = self._second_largest(children)
            for i in range(num_children):
                row, = np.where(children==i)
                if len(row)==0:
                    props = {}
                else:
                    props = {'parent': proxy_object.IncompleteProxyObjectFromFinderId(parents[row[0]],'group'),
                             'original_subgroup_number': original[row[0]]}
                yield [i]+[props.get(n, None) for n in property_names]
        else:
            # must be group
            num_parents = self._second_largest(parents)
            for i in range(num_parents):
                row, = np.where(parents==i)
                children_proxies = [proxy_object.IncompleteProxyObjectFromFinderId(child, 'halo') for child in children[row]]
                if len(children_proxies)==0:
                    props = {}
                else:
                    props = {'child': children_proxies}
                yield [i]+[props.get(n, None) for n in property_names]

