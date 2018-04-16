"""Support for the directory structure used by Eagle-like runs"""

from .pynbody import PynbodyInputHandler
import os
import re
from .. import config


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
            return os.path.join(os.path.dirname(path), "particledata_%s/eagle_subfind_particles_%s" % (snap_id, snap_id))
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
        if object_typetag!= 'halo':
            raise ValueError("Unknown object type %r" % object_typetag)
        f = self.load_timestep(ts_extension)

        halofilepath = self._extension_to_halodata_filename(ts_extension)
        f_subfind = pynbody.load(halofilepath)
        h = f_subfind.halos()
        f._db_current_halocat = h

        return h