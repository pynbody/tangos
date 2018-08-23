"""input_handlers sub-package

This sub-package handles returning views of the original simulation data on disk for further processing.
At the moment, the views are always pynbody views, but adaptors for other frameworks could be implemented by
overriding the functionality.

For an introduction, see https://pynbody.github.io/tangos/input_handlers.html
"""

from __future__ import absolute_import
import os, os.path
from .. import config
from ..log import logger
import importlib
import weakref
import warnings

class DummyTimeStep(object):
    def __init__(self, filename):
        self.filename = filename


    def __repr__(self):
        return self.filename

    pass


_loaded_timesteps = weakref.WeakValueDictionary()


class HandlerBase(object):
    """This class handles the output from a simulation as it resides on disk.

    Subclasses provide implementations for different formats and situations.
    """

    def __init__(self, basename):
        self.basename = self.strip_slashes(basename)
        self.quicker = False # a flag to indicate that corners may be cut in the interest of efficiency

    @classmethod
    def best_matching_handler(cls, basename):
        """Find the best subclass to read in the specified folder of simulation timesteps"""
        return cls

    def enumerate_timestep_extensions(self):
        """Yield the extension of each timestep available on disk"""
        raise NotImplementedError

    def get_properties(self):
        """Returns a dictionary of properties of the simulation"""
        return {}

    def get_timestep_properties(self, ts_extension):
        """Returns a dictionary of properties of the timestep"""
        return {}

    def enumerate_objects(self, ts_extension, object_typetag='halo', min_halo_particles=None):
        """Yield halo_number, NDM, NStar, Ngas for halos in the specified timestep"""
        for X in self._enumerate_objects_from_statfile(ts_extension, object_typetag):
            yield X

    def _enumerate_objects_from_statfile(self, ts_extension, object_typetag):
        """Implementation of enumerate_objects when the information is provided by a file readable
        by the halo_stat_files module.

        Call from subclasses when this behaviour is desired"""
        statfile = self.get_stat_file(ts_extension, object_typetag)
        logger.info("Reading halos for timestep %r using a stat file", ts_extension)
        for X in statfile.iter_rows("n_dm", "n_star", "n_gas"):
            yield X

    def _can_enumerate_objects_from_statfile(self, ts_extension, object_typetag):
        """Returns True if the objects can be enumerated from a stat file"""
        try:
            self.get_stat_file(ts_extension, object_typetag)
            return True
        except IOError:
            return False

    def get_stat_file(self, ts_extension, object_typetag):
        from . import halo_stat_files
        if object_typetag != 'halo':
            raise IOError("No stat file known for object type %s"%object_typetag)
        #ts = DummyTimeStep()
        #ts.redshift = self.get_timestep_properties(ts_extension)['redshift']
        from . import caterpillar
        statfile = halo_stat_files.HaloStatFile(self._extension_to_filename(ts_extension))
        return statfile


    def available_object_property_names_for_timestep(self, ts_extension, object_typetag):
        """Returns a list of all pre-computed properties available for this timestep.

        These pre-computed properties can then be evaluated through iterate_object_properties_for_timestep"""
        return self.get_stat_file(ts_extension, object_typetag).all_columns()

    def iterate_object_properties_for_timestep(self, ts_extension, object_typetag, property_names):
        """Iterate through all objects of specified type, providing named pre-computed data.

        This is normally data that was calculated and stored by the halo finder such as masses or particle counts.
        Each object yields an array with the finder_id followed by values for the requested properties.

        :arg ts_extension - the timestep path (relative to the simulation basename)
        :arg object_typetag - the type of halo catalogue (e.g. 'halo' or 'group')
        :arg property_names - a list of property names to retrieve (depends on catalogue file format)
        """
        statfile = self.get_stat_file(ts_extension, object_typetag)
        for values in statfile.iter_rows(*property_names):
            yield values


    def load_timestep(self, ts_extension, mode=None):
        """Returns an object that connects to the data for a timestep on disk -- possibly a version cached in
        memory"""
        ts_hash = hash((ts_extension,mode,type(self)))
        stored_timestep = _loaded_timesteps.get(ts_hash, None)
        if stored_timestep is not None:
            return stored_timestep
        else:
            data = self.load_timestep_without_caching(ts_extension, mode=mode)
            _loaded_timesteps[ts_hash] = data
            return data

    def load_region(self, ts_extension, region_specification, mode=None):
        """Returns an object that connects to the data for a timestep on disk, filtered using the
        specified region specification. Acceptable region specifications are output handler dependent.

        The returned object may be a sub-view of a cached in-memory timestep."""
        raise NotImplementedError

    def load_timestep_without_caching(self, ts_extension, mode=None):
        """Creates and returns an object that connects to the data for a timestep on disk"""
        raise NotImplementedError

    def load_object(self, ts_extension, halo_number, object_typetag='halo', mode=None):
        """Creates and returns an object that connects to the data for a halo on disk.

        :arg ts_extension - the timestep path (relative to the simulation basename)

        :arg halo_number - the halo number in the raw halo finder output

        :arg object_typetag - the type of halo catalogue (normally 'halo' or, for Subfind, might be 'group' to get
                                                     the top-level groups)

        :arg mode - sets a method for loading the halo, which is dependent on being implemented by the underlying
                    file manager. Current options (for the default pynbody handler) are

                     * None: load entire snapshot from disk, then return halo particles
                     * 'partial': load only the halo particles from disk
                     * 'server': use a remote server model to retrieve the particles
        """
        raise NotImplementedError

    def load_tracked_region(self, ts_extension, track_data, mode=None):
        """Creates and returns an object that connects to the on-disk data for the specified tracked region.

        :arg mode - sets a method for loading the tracked region; see load_object mode for more information"""
        raise NotImplementedError


    @classmethod
    def handler_class_name(cls):
        module = cls.__module__
        if module.startswith(HandlerBase.__module__):
            submodule = module[len(HandlerBase.__module__)+1:]
            return submodule+"."+cls.__name__
        else:
            return module+"."+cls.__name__

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


def _map_deprecated_handler_name(handler):
    if 'OutputSetHandler' in handler:
        new_handler = handler.replace('OutputSetHandler','InputHandler')
        warnings.warn("The database has stored the handler name as %r; automatically translating this to the new name %r"%(handler, new_handler),
                  DeprecationWarning)
        return new_handler
    else:
        return handler

def get_named_handler_class(handler):
    """Get a HandlerBase identified by the given name.

    The name is of the format submodule.ClassName

    :rtype HandlerBase"""
    handler = _map_deprecated_handler_name(handler)
    try:
        output_module = importlib.import_module('.'+handler.split('.')[0],__name__)
    except ImportError:
        output_module = importlib.import_module(handler.split('.')[0])
    output_class = getattr(output_module, handler.split('.')[1])
    return output_class
