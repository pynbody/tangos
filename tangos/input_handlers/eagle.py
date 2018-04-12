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
    def _pynbody_path_from_snapdir_path(cls, path):
        snap_id = cls._snap_id_from_snapdir_path(path)
        if snap_id:
            x= os.path.join(path, "snap_%s" % snap_id)
            print x
            return x
        else:
            raise IOError("Cannot infer correct path to pass to pynbody")

    def _extension_to_filename(self, ts_extension):
        return str(os.path.join(config.base, self.basename, self._pynbody_path_from_snapdir_path(ts_extension)))
