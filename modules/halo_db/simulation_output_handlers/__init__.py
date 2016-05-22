"""simulation_output_handlers sub-package

This sub-package handles returning views of the original simulation data on disk for further processing.
At the moment, the views are always pynbody views, but adaptors for other frameworks could be implemented by
overriding the functionality.
"""

import os, os.path
from .. import config
import importlib
import weakref

_loaded_timesteps = {}


class SimulationOutputSetHandler(object):
    """This class handles the output from a simulation as it resides on disk.

    Subclasses provide implementations for different formats and situations.
    """

    def __init__(self, basename):
        self.basename = self.strip_slashes(basename)

    def enumerate_timestep_extensions(self):
        """Yield the extension of each timestep available on disk"""
        raise NotImplementedError

    def get_properties(self):
        """Returns a dictionary of properties of the simulation"""
        raise NotImplementedError

    def get_timestep_properties(self, ts_extension):
        """Returns a dictionary of properties of the timestep"""
        raise NotImplementedError

    def enumerate_halos(self, ts_extension):
        """Yield halo_number, NDM, NStar, Ngas for halos in the specified timestep"""
        raise NotImplementedError

    def load_timestep(self, ts_extension):
        """Returns an object that connects to the data for a timestep on disk -- possibly a version cached in
        memory"""
        ts_filename = self._extension_to_filename(ts_extension)
        stored_timestep = _loaded_timesteps.get(ts_filename, lambda: None)()
        if stored_timestep is not None:
            return stored_timestep
        else:
            data = self.load_timestep_without_caching(ts_extension)
            _loaded_timesteps[ts_filename] = weakref.ref(data)
            return data

    def load_timestep_without_caching(self, ts_extension):
        """Creates and returns an object that connects to the data for a timestep on disk"""
        raise NotImplementedError

    def load_halo(self, ts_extension, halo_number, partial=False):
        """Creates and returns an object that connects to the data for a halo on disk.

        :arg partial - if True, attempt to load *only* the data for the halo
          (i.e. don't return a view of the parent timestep)"""
        raise NotImplementedError

    def load_tracked_region(self, ts_extension, track_data, partial=False):
        """Creates and returns an object that connects to the on-disk data for the specified tracked region.

        :arg partial - if True, attempt to load *only* the data for the region (i.e. don't return a view
           of the parent timestep)"""
        raise NotImplementedError


    @classmethod
    def handler_class_name(cls):
        module = cls.__module__
        assert module.startswith(SimulationOutputSetHandler.__module__)
        submodule = module[len(SimulationOutputSetHandler.__module__)+1:]
        return submodule+"."+cls.__name__

    @staticmethod
    def strip_slashes(name):
        """Strip trailing and leading slashes from relative path"""
        if len(name) == 0: return name
        while name[0] == "/":
            name = name[1:]
            if len(name) == 0: return name
        while name[-1] == "/":
            name = name[:-1]
            if len(name) == 0: return name
        return name

    def _extension_to_filename(self, ts_extension):
        """Given the timestep extension, form the path to the actual file"""
        # Explicit str cast - pynbody seems inconsistent about how it responds to unicode filenames
        return str(os.path.join(config.base, self.basename, ts_extension))


def get_named_handler_class(handler):
    """Get a SimulationOutputSetHandler identified by the given name.

    The name is of the format submodule.ClassName

    :rtype SimulationOutputSetHandler"""
    output_module = importlib.import_module('.'+handler.split('.')[0],__name__)
    output_class = getattr(output_module, handler.split('.')[1])
    return output_class
