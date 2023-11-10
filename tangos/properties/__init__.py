import functools
import importlib
import os
import sys
import warnings
from importlib.metadata import entry_points

import numpy as np

from .. import input_handlers, parallel_tasks
from ..log import logger
from ..util import timing_monitor


class PropertyCalculationMetaClass(type):
    # Present to register new subclasses of PropertyCalculation, so that subclasses can be dynamically
    # instantiated when required based on their cls.names values. Stored as a dictionary so that
    # reloaded classes overwrite their old versions.
    def __init__(cls, name, bases, dict):
        type.__init__(cls, name, bases, dict)
        if hasattr(cls, 'name'):
            warnings.warn("%r defines a name() class method which is deprecated. "
                          "Use instead a class static variable 'names'."%cls, DeprecationWarning, stacklevel=2)
            cls.names = cls.name()
        if callable(cls.requires_particle_data):
            warnings.warn("%r defines a requires_particle_data() class method which is deprecated; "
                          "it should instead be a class static variable", DeprecationWarning, stacklevel=2)
            cls.requires_particle_data = cls.requires_particle_data()
        if cls.names is not None:
            cls._all_classes.append(cls)

class PropertyCalculation(metaclass=PropertyCalculationMetaClass):
    _all_classes = []

    # In child class, defines the most general handler that this property is compatible with.
    # If unchanged, it is compatible with all handlers. Typically it will be appropriate to specify something more
    # restrictive e.g. input_handlers.pynbody.PynbodyInputHandler
    works_with_handler = input_handlers.HandlerBase

    # Specifies whether the particle data needs to be provided for this class to perform a calculation; if
    # False, only existing PropertyCalculation are required by this calculation (see requires_property below).
    requires_particle_data = False

    # Specifies a tuple of names of properties that will be calculated by this class.
    names = None

    # object to help cache recognise that there is no result in the database
    # (avoids re-querying the database for missing objects)
    __no_result = object()

    @classmethod
    def all_classes(cls):
        return cls._all_classes

    def __init__(self, simulation):
        """Initialise a PropertyCalculation calculation object

        :param simulation: The simulation from which the properties will be derived
        :type simulation: tangos.core.simulation.Simulation
        """
        self.__simulation = simulation
        self.__simulation_property_cache = {}
        self.timing_monitor = timing_monitor.TimingMonitor()

    def get_simulation_property(self, name, default):
        """Gets a property of the simulation on which this calculation is being computed

        :param name: The name of the property to retrieve
        :param default: The default value to return if no such property exists

        This is safe to call even if the database might be locked. It also implements a
        cache at the object level, so in the unlikely event that a simulation property is
        updated mid-calculation, it may return old results."""
        if name not in self.__simulation_property_cache:
            self.__simulation_property_cache[name] = \
                self._get_simulation_property_uncached(name, self.__no_result)
        result = self.__simulation_property_cache[name]
        if result is self.__no_result:
            return default
        else:
            return result

    def _get_simulation_property_uncached(self, name, default):
        """Query the database for a property of the simulation on which this calculation is being computed

        :param name: The name of the property to retrieve
        :param default: The default value to return if no such property exists

        This is safe to call even if the database might be locked.
        """
        with parallel_tasks.lock.SharedLock("insert_list"):
            return self.__simulation.get(name, default)

    @classmethod
    def index_of_name(cls, name):
        """Returns the index of the named property in the
        results returned from calculate().

        For example, for a BasicPropertyCalculation object X,
        X.calculate(..)[X.index_of_name("SSC")] returns the SSC.
        """
        name = name.split("(")[0]
        return cls.names.index(name)

    @classmethod
    def no_proxies(self):
        """Returns True if the properties MUST be supplied
        as an actual Halo object rather than the normal
        dictionary-like proxy that is supplied. Only use if
        absolutely necessary; has adverse consequences e.g.
        because uncommitted updates are not reflected into
        your calculate function"""
        return False

    def requires_property(self):
        """Returns a list of existing properties
        required to calculate this property"""
        return []

    def preloop(self, sim, db_timestep):
        """Perform one-per-snapshot calculations, given the loaded simulation data and TimeStep object"""
        pass

    def region_specification(self, db_data):
        """Returns an abstract specification of the region that this halo property is to be calculated on,
        or None if we want the halo particles as defined by the finder.

        See spherical_region.SphericalRegionPropertyCalculation for an example and useful base class for returning
        everything within the virial radius."""
        return None


    def mark_timer(self, label=None):
        """Called by subfunctions to mark a time"""
        self.timing_monitor.mark(label)


    def accept(self, db_entry):
        for x in self.requires_property():
            if db_entry.get(x, None) is None:
                return False
        return True

    def calculate(self, particle_data, halo_entry):
        """Calculate the properties using the given data

        :param particle_data: The raw particle data, if available
        :type particle_data: pynbody.snapshot.SimSnap (when the pynbody backend is in use, otherwise could be a yt snapshot etc)

        :param halo_entry: The database object associated with the halo, if available
        :type halo_entry: tangos.core.halo.SimulationObjectBase
        :return: All properties as named by names()
        """
        raise NotImplementedError

    def live_calculate(self, halo_entry, *input_values):
        """Calculate the result of a function, using the existing data in the database alone

        :param halo_entry: The database object associated with the halo
        :type halo_entry: tangos.core.halo.SimulationObjectBase

        :param input_values: Input values for the function
        :return: All function values as named by self.names
        """
        if self.requires_particle_data:
            raise(RuntimeError("Cannot live-calculate a property that requires particle data"))
        return self.calculate(None, halo_entry)

    def live_calculate_named(self, name, halo_entry, *input_values):
        """Calculate the result of a function, using the existing data in the database alone

        :param name: The name of the one property to return (which must be one of the values specified by self.names)
        :param halo_entry: The database object associated with the halo
        :type halo_entry: tangos.core.halo.SimulationObjectBase

        :param input_values: Input values for the function
        :return: The single named value
        """
        values = self.live_calculate(halo_entry, *input_values)
        names = self.names
        if isinstance(names, str):
            return values
        else:
            return values[self.names.index(name)]

    def calculate_from_db(self, db):
        if self.requires_particle_data:
            region_spec =  self.region_specification(db)
            if region_spec:
                halo_particles = db.timestep.load_region(region_spec)
            else:
                halo_particles = db.load()

            preloops_done = getattr(halo_particles.ancestor, "_did_preloop", [])
            halo_particles.ancestor._did_preloop = preloops_done

            if str(self.__class__) not in preloops_done:
                self.preloop(halo_particles.ancestor, db.timestep)
                preloops_done.append(str(self.__class__))
        else:
            halo_particles = None
        return self.calculate(halo_particles, db)

    def plot_x_values(self, for_data):
        """Return a suitable array of x values to match the
        given y values"""
        return np.arange(self.plot_x0(), self.plot_x0() + self.plot_xdelta() * (len(for_data) - 0.5), self.plot_xdelta())

    def plot_x_extent(self):
        return None

    def plot_extent(self):
        return None

    def plot_x0(self):
        return 0

    def plot_xdelta(self):
        return 1.0

    def get_interpolated_value(self, at_x_position, property_array):
        """Return the value of the property at the given x position"""
        x0 = self.plot_x0()
        delta_x = self.plot_xdelta()

        # linear interpolation
        i0 = int((at_x_position - x0) / delta_x)
        i1 = i0 + 1

        i0_loc = float(i0) * delta_x + x0
        i1_weight = (at_x_position - i0_loc) / delta_x
        i0_weight = 1.0 - i1_weight

        if i1 >= len(property_array) or i0 < 0:
            return None
        else:
            return property_array[i0] * i0_weight + property_array[i1] * i1_weight

    def plot_xlabel(self):
        return None

    def plot_ylabel(self):
        return None

    def plot_yrange(self):
        return None

    def plot_xlog(self):
        return True

    def plot_ylog(self):
        return True

    def plot_clabel(self):
        return None

HaloProperties = PropertyCalculation # old name, to be deprecated

class TimeChunkedProperty(PropertyCalculation):
    """TimeChunkedProperty implements a special type of halo property where chunks of a histogram are stored
    at each time step, then appropriately reassembled when the histogram is retrieved.

    For more information see docs/histogram_properties.md"""

    pixel_delta_t_Gyr = 0.02  # default value. Can be overriden by a simulation property histogram_delta_t_Gyr
    minimum_store_Gyr = 0.5

    def __init__(self, simulation):
        self.pixel_delta_t_Gyr = simulation.get("histogram_delta_t_Gyr", self.pixel_delta_t_Gyr)
        super().__init__(simulation)


    def bin_index(self, time):
        """Convert a time (Gyr) to a bin index in the histogram"""
        index = int(time/self.pixel_delta_t_Gyr)
        if index<0:
            index = 0
        return index

    def store_slice(self, time):
        """Tells subclasses which have generated a histogram over all time which slice of that histogram
        they should store."""
        return slice(self.bin_index(time-self.minimum_store_Gyr), self.bin_index(time))

    def reassemble(self, property, reassembly_type='major'):
        """Reassemble a histogram by suitable treatment of the merger tree leading up to the current halo.

        This function is normally called by the framework (see SimulationObjectBase.get_data_with_reassembly_options) and you would
        rarely call it directly yourself. From within a live-calculation, it can be accessed using the
        reassemble(halo_property, options...) function. See live_calculation.md for more information.

        :param: property - the halo property for which the reassembly should occur

        :param: reassembly_type - if 'major' (default), return the histogram for the major progenitor branch
                                - if 'sum', return the histogram summed over all progenitors
                                (e.g. this can be used to return SFR histograms that count infalling material as well
                                as the major progenitor)
                                - if 'place', return only the histogram stored at this step but place it within
                                a correctly zero-padded array
                                - if 'raw', return the raw data
        """

        from tangos import relation_finding as rfs

        if reassembly_type=='major':
            return self._reassemble_using_finding_strategy(property, strategy = rfs.MultiHopMajorProgenitorsStrategy)
        elif reassembly_type=='major_across_simulations':
            return self._reassemble_using_finding_strategy(property, strategy = rfs.MultiHopMajorProgenitorsStrategy,
                                                           strategy_kwargs = {'target': None, 'one_simulation': False})
        elif reassembly_type=='sum':
            return self._reassemble_using_finding_strategy(property, strategy = rfs.MultiHopAllProgenitorsStrategy)
        elif reassembly_type=='place':
            return self._place_data(property.halo.timestep.time_gyr, property.data_raw)
        elif reassembly_type=='raw':
            return property.data_raw
        else:
            raise ValueError("Unknown reassembly type")

    def _place_data(self, time, raw_data):
        final = np.zeros(self.bin_index(time))
        end = len(final)
        start = end - len(raw_data)
        final[start:] = raw_data
        return final

    def _reassemble_using_finding_strategy(self, property, strategy, strategy_kwargs={}):
        name = property.name.text
        halo = property.halo
        t, stack = halo.calculate_for_descendants("t()", "raw(" + name + ")", strategy=strategy, strategy_kwargs=strategy_kwargs)
        final = np.zeros(self.bin_index(t[0]))
        previous_time = -1
        for t_i, hist_i in zip(t, stack):
            end = self.bin_index(t_i)
            start = end - len(hist_i)
            valid = hist_i == hist_i
            if t_i != previous_time:
                # new timestep; overwrite what was there previously
                final[start:end][valid] = hist_i[valid]
            else:
                # same timestep, multiple halos; accumulate
                final[start:end][valid] += hist_i[valid]
            previous_time = t_i
        return final


    def plot_xdelta(self):
        return self.pixel_delta_t_Gyr

    def plot_xlog(self):
        return False

    def plot_ylog(self):
        return False



class LivePropertyCalculation(PropertyCalculation):
    requires_particle_data = False

    def __init__(self, simulation, *args):
        super().__init__(simulation)
        self._nargs = len(args)

    def calculate(self, _, halo):
        return self.live_calculate(halo, *([None]*self._nargs))

LiveHaloProperties = LivePropertyCalculation # old name, to be deprecated

class LivePropertyCalculationInheritingMetaProperties(LivePropertyCalculation):
    """LivePropertyCalculation which inherit the meta-data (i.e. x0, delta_x values etc) from
    one of the input arguments"""
    def __init__(self, simulation, inherits_from, *args):
        """
        :param simulation: The simulation DB entry for this instance
        :param inherits_from: The PropertyCalculation description from which the metadata should be inherited
        :type inherits_from: PropertyCalculation
        """
        super().__init__(simulation)
        self._inherits_from = inherits_from(simulation)

    def plot_x0(self):
        return self._inherits_from.plot_x0()

    def plot_xdelta(self):
        return self._inherits_from.plot_xdelta()


##############################################################################
# UTILITY FUNCTIONS
##############################################################################

def all_property_classes():
    """Return list of all classes derived from PropertyCalculation"""

    return PropertyCalculation.all_classes()



def _check_class_provided_name(name):
    if "(" in name or ")" in name:
        raise ValueError("Property names must not include brackets; %s not suitable"%name)

def all_properties(with_particle_data=True):
    """Return list of all properties which can be calculated using classes derived from PropertyCalculation"""
    classes = all_property_classes()
    pr = []
    for c in classes:
        if c.requires_particle_data and not with_particle_data:
            continue

        name = c.names
        if isinstance(name, str):
            _check_class_provided_name(name)
            pr.append(name)
        else:
            for name_j in name:
                _check_class_provided_name(name_j)
                pr.append(name_j)

    return pr

@functools.lru_cache
def providing_class(property_name, handler_class=None, silent_fail=False, explain=False):
    """Return property calculator class for given property name when files will be loaded by specified handler.

    :param property_name -- name of property to be calculated
    :param handler_class -- class of handler that will be used to load files
                            (e.g. input_handlers.pynbody.PynbodyInputHandler).
                            If None, return "live" properties which can be calculated without particle data.
    :param silent_fail -- if True, return None if no class is found, otherwise raise NameError
    :param explain -- if True, print out the reason why a particular class was selected

    When more than one possible class is capable of calculating the requested property, the following criteria
    are used to select one. The guiding criterion is to select user-provided code of the greatest specificity.

    1) If possible, the class targetting the most specialised input handler is selected. That is, if a
       class targetting say PynbodyInputHandler is available, it will be selected in preference to one
       targetting HandlerBase.
    2) Next, the class hierarchy of the properties themselves is inspected. If one class is a subclass of another,
       the more specialised class is selected. For example, if there are two classes calculating "my_prop", A and B,
       and B is a child class of A, B is selected.
    2) If there is no class hierarchy, the class defined in the tangos codebase is de-prioritised over any externally
       provided classes
    3) If there is still a tie, the string representation of the classname (including the module) is used to sort
       alphabetically. This has no particular rationale except to make reproducible results.

    """

    candidates_unfiltered = all_providing_classes(property_name)

    if handler_class is None:
        candidates = list(filter(lambda c: not c.requires_particle_data, candidates_unfiltered))
    else:
        candidates = []
        for c in candidates_unfiltered:
            if issubclass(handler_class, c.works_with_handler):
                candidates.append(c)
            else:
                if explain:
                    logger.info(f"Property class selection: {c.__module__}.{c.__qualname__} is excluded, as it is not compatible with handler {handler_class}")

    if len(candidates)>=1:
        # return the property which is most specialised
        _sort_by_class_hierarchy(candidates, explain)
        return candidates[0]
    elif silent_fail:
        return None
    else:
        raise NameError("No providing class for property " + property_name)

@functools.lru_cache
def all_providing_classes(property_name):
    """Return all the calculator classes for the given property name (possibly multiple, for different handlers)"""
    classes = all_property_classes()
    property_name = property_name.lower()
    candidates = []
    for c in classes:
        name = c.names
        if isinstance(name, tuple) or isinstance(name, list):
            for name_j in name:
                if name_j.lower() == property_name:
                    candidates.append(c)
        elif name.lower() == property_name:
            candidates.append(c)
    return candidates


def _sort_by_class_hierarchy(candidates, explain=False):
    def explanation(s):
        if explain:
            logger.info("Property class selection: "+s)
    def cmp(a, b):
        a_name = a.__module__ + "." + a.__qualname__
        b_name = b.__module__ + "." + b.__qualname__

        if a is b:
            return 0

        # Rule 1: prefer the most specialised handler
        if a.works_with_handler is not b.works_with_handler:
            if issubclass(a.works_with_handler, b.works_with_handler):
                explanation(f"{a_name} is preferred to {b_name} because handler ({a.works_with_handler}) is a subclass "
                      f"of handler {b.works_with_handler}")
                return -1
            elif issubclass(b.works_with_handler, a.works_with_handler):
                explanation(f"{b_name} is preferred to {a_name} because handler ({b.works_with_handler}) is a subclass "
                      f"of handler {a.works_with_handler}")
                return 1

        # Rule 2: prefer the most specialised class:
        if issubclass(a, b):
            explanation(f"{a_name} is preferred to {b_name} because it is a subclass of {b_name}")
            return -1
        elif issubclass(b, a):
            explanation(f"{b_name} is preferred to {a_name} because it is a subclass of {a_name}")
            return 1

        # Rule 3: prefer externally-provided classes over tangos-provided ones

        if a.__module__.startswith("tangos.") and not b.__module__.startswith("tangos."):
            explanation(f"{b_name} is preferred to {a_name} because it is provided externally to tangos")
            return 1
        elif b.__module__.startswith("tangos.") and not a.__module__.startswith("tangos."):
            explanation(f"{a_name} is preferred to {b_name} because it is provided externally to tangos")
            return -1

        # Rule 4: out of sensible ways to order, now we just go alphabetical
        a_name = a.__module__ + "." + a.__qualname__
        b_name = b.__module__ + "." + b.__qualname__
        if a_name<b_name:
            explanation(f"{a_name} is preferred to {b_name} because of alphabetical ordering")
            return -1
        elif a_name>b_name:
            explanation(f"{b_name} is preferred to {a_name} because of alphabetical ordering")
            return 1

        explanation(f"{a_name} and {b_name} could not be distinguished by any of the ordering rules")
        # very surprising to reach this - how can two different classes have the same module and name?
        return 0

    candidates.sort(key=functools.cmp_to_key(cmp))



def providing_classes(property_name_list, handler_class, silent_fail=False, explain=False):
    """Return classes for given list of property names; see providing_class for details"""
    classes = []
    for property_name in property_name_list:
        cl = providing_class(property_name, handler_class, silent_fail, explain)
        if cl not in classes and cl is not None:
            classes.append(cl)

    return classes

def instantiate_classes(simulation, property_name_list, silent_fail=False, explain=False):
    """Instantiate appropriate property calculation classes for a given simulation and list of property names."""
    instances = []
    handler_class = type(simulation.get_output_handler())
    for property_identifier in property_name_list:
        instances.append(providing_class(property_identifier, handler_class, silent_fail, explain)(simulation))

    return instances

def instantiate_class(simulation, property_name, silent_fail=False):
    """Instantiate an appropriate property calculation class for a given simulation and property name."""
    instance = instantiate_classes(simulation, [property_name], silent_fail)
    if len(instance)==0:
        return None
    else:
        return instance[0]

def _get_entry_points():
    if sys.version_info >= (3, 10):
        return entry_points(group='tangos.property_modules')
    else:
        return entry_points().get('tangos.property_modules', [])

def _import_configured_property_modules():
    if "PYTEST_CURRENT_TEST" in os.environ:
        warnings.warn("Not importing external property modules during testing", ImportWarning)
        return

    from ..config import property_modules
    for pm in property_modules:
        if pm=="": continue
        try:
            importlib.import_module(pm)
        except ImportError:
            warnings.warn("Failed to import requested property module %r. Some properties may be unavailable."%pm,
                          ImportWarning)
    for module in _get_entry_points():
        module.load()

_import_configured_property_modules()
from . import intrinsic, live_profiles, pynbody, yt
