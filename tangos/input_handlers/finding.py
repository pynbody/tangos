import fnmatch
import glob
import os.path

import numpy as np

from .. import config
from ..log import logger
from ..util.subclasses import find_subclasses


def find(extension=None, mtd=None, ignore=None, basename="", patterns=[]):
    if mtd == None:
        mtd = config.max_traverse_depth
    if ignore == None:
        ignore = config.file_ignore_pattern
    out = []

    if extension is not None:
        for d in range(mtd + 1):
            out += glob.glob(basename + ("*/" * d) + "*." + extension)

        out = [f[:-(len(extension) + 1)] for f in out]

        out = [f for f in out if not any([fnmatch.fnmatch(f, ipat) for ipat in ignore])]
    else:
        for d in range(mtd + 1):
            for pattern in patterns:
              out += glob.glob(basename + ("*/" * d) + pattern)


    return set(out)


class PatternBasedFileDiscovery:
    """Provides methods for pattern-based file discovery, i.e. glob-ing for specific patterns of file in this
    folder and sub-folders"""

    patterns = []  # should be specified by child class. See input_handlers.pynbody for examples.

    auxiliary_file_patterns = []
    # will not be used for finding the snapshots, but if files matching these patterns are present
    # the handler is more likely to be selected automatically. See e.g. GadgetRockstarInputHandler.

    @classmethod
    def best_matching_handler(cls, basename):
        handler_names = []
        handler_timestep_lengths = []
        base = os.path.join(config.base, basename)

        # Add all subclasses, sub-subclasses, sub-subclasses, ...
        all_possible_handlers = find_subclasses(cls)
        if not all_possible_handlers:
            return cls

        for possible_handler in all_possible_handlers:
            timesteps_detected = find(basename=base + "/", patterns=possible_handler.patterns)
            other_files_detected = find(basename=base+"/", patterns=possible_handler.auxiliary_file_patterns)
            handler_names.append(possible_handler)
            handler_timestep_lengths.append(len(timesteps_detected)+len(other_files_detected))
        best_handler = handler_names[np.argmax(handler_timestep_lengths)]
        logger.debug("Detected best handler (of %d) is %s",len(all_possible_handlers), best_handler)

        return best_handler

    def enumerate_timestep_extensions(self, parallel=False):
        base = os.path.join(config.base, self.basename)

        def find_matching_extensions():
            logger.info("Enumerate timestep extensions base=%r patterns=%s", base, self.patterns)
            results = sorted(find(basename=base + "/", patterns=self.patterns))
            logger.info("Found %d candidates to be inspected", len(results))
            return results


        if parallel:
            from ..parallel_tasks import jobs
            extensions = jobs.generate_task_list_and_parallel_iterate(find_matching_extensions)
        else:
            extensions = find_matching_extensions()

        for e in extensions:
            if self._is_able_to_load(self._transform_extension(e)):
                yield self._transform_extension(e[len(base) + 1:])
            else:
                logger.info("Could not load %s with class %s", e, self)

    def _is_able_to_load(self, fname):
        """Determine whether a named file can be loaded

        Override in child class to filter the pattern-based file matches"""
        return True

    def _transform_extension(self, extension_name):
        """Can be overriden by child classes to map from the literal filename discovered to a different name for loading"""
        return extension_name
