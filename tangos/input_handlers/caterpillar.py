from __future__ import absolute_import

import re
from .. import config
from .pynbody import PynbodyInputHandler
from . import halo_stat_files
import os.path
import pynbody

class CaterpillarInputHandler(PynbodyInputHandler):
    patterns = ["snapdir_???"]

    @classmethod
    def _snap_id_from_snapdir_path(cls, path):
        match = re.match(".*snapdir_([0-9]{3})/?", path)
        if match:
            return int(match.group(1))
        else:
            return None

    @classmethod
    def _pynbody_path_from_snapdir_path(cls, path):
        snap_id = cls._snap_id_from_snapdir_path(path)
        if snap_id:
            return os.path.join(path, "snap_%3d" % snap_id)
        else:
            raise IOError("Cannot infer correct path to pass to pynbody")

    @classmethod
    def _rockstar_path_from_snapdir_path(cls, path):
        snap_id = cls._snap_id_from_snapdir_path(path)
        if snap_id:
            return os.path.join(os.path.dirname(os.path.dirname(path)),
                                "halos", "halos_%d"%snap_id)
        else:
            raise IOError("Cannot infer path of halos")

    def _extension_to_filename(self, ts_extension):
        return str(os.path.join(config.base, self.basename, self._pynbody_path_from_snapdir_path(ts_extension)))

    def _is_able_to_load(self, filepath):
        try:
            f = pynbody.load(self._pynbody_path_from_snapdir_path(filepath))
            h = pynbody.halo.RockstarCatalogue(f, pathname=self._rockstar_path_from_snapdir_path(filepath),
                                               format_revision='caterpillar')
            return True
        except (IOError, RuntimeError):
            return False

class CaterpillarRockstarStatFile(halo_stat_files.RockstarStatFile):

    @classmethod
    def filename(cls, timestep):
        orig_dirname = os.path.dirname(timestep.filename)
        stepid = CaterpillarInputHandler._snap_id_from_snapdir_path(orig_dirname)
        filename = "out_%d.list"%stepid
        return os.path.join(CaterpillarInputHandler._rockstar_path_from_snapdir_path(orig_dirname),
                            filename)