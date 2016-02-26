import numpy as np
import math
import time
import inspect
import pyparsing as pp


class HaloProperties(object):

    def requires_array(self):
        """Returns a list of loaded arrays required to
        calculate this property"""
        return []

    @classmethod
    def plot_x_values(cls, for_data):
        """Return a suitable array of x values to match the
        given y values"""
        return np.arange(cls.plot_x0(),  cls.plot_x0()+cls.plot_xdelta()*(len(for_data)-0.5), cls.plot_xdelta())

    @classmethod
    def requires_simdata(self):
        """If this returns false, the class can do its
        calculation without any raw simulation data loaded
        (i.e. derived from other properties)"""
        return True

    @classmethod
    def name(self):
        """Returns either the name or a list of names of
        properties that will be calculated by this class"""
        return "undefined"

    @classmethod
    def index_of_name(cls, name):
        """Returns the index of the named property in the
        results returned from calculate().

        For example, for a BasicHaloProperties object X,
        X.calculate(..)[X.index_of_name("SSC")] returns the SSC.
        """
        name = name.split("(")[0]
        return cls.name().index(name)

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

    def preloop(self, sim, filename, property_array):
        """Perform one-time pre-halo-loop calculations,
        given entire simulation SimSnap, filename
        and existing property array"""

    def spherical_region(self):
        """Returns a boolean specifying
        whether the host is to provide a spherical, virial, centred
        region (if True) based on the halo; or (if False) the
        actual group file defining the halo (which may exclude
        subhalos for instance)."""
        return False

    def start_timer(self):
        """Start a timer. Can be overriden by subclasses to provide
        more useful timing details"""

        self._time_marks_info = ["start"]
        self._time_start = time.time()
        self._time_marks = [time.time()]

    def end_timer(self):
        """End the timer and return the intervening time."""

        if len(self._time_marks) == 1:
            return time.time() - self._time_start
        else:
            self._time_marks_info.append("end")
            type(self)._time_marks_info = self._time_marks_info
            self._time_marks.append(time.time())
            return np.diff(self._time_marks)

    def mark_timer(self, label=None):
        """Called by subfunctions to mark a time"""

        self._time_marks.append(time.time())
        if label is None:
            self._time_marks_info.append(
                inspect.currentframe().f_back.f_lineno)
        else:
            self._time_marks_info.append(label)

    def accept(self, db_entry):
        for x in self.requires_property():
            if db_entry.get(x, None) is None:
                return False
        return True

    def calculate(self,  halo, existing_properties):
        """Performs calculation, given halo, and returns
        the property to be stored under dict[name()]."""



    def calculate_from_db(self, db):
        import pynbody
        if self.requires_simdata():
            h = db.load()
            h.physical_units()
            preloops_done = getattr(h.ancestor, "_did_preloop", [])
            h.ancestor._did_preloop = preloops_done
            if str(self.__class__) not in preloops_done:
                self.preloop(h.ancestor, h.ancestor.filename, db)
                preloops_done.append(str(self.__class__))
            if self.spherical_region():
                gp_sp = h.ancestor[pynbody.filt.Sphere(db['Rvir'], db['SSC'])]
            else:
                gp_sp = h
        else:
            gp_sp = None
        self.start_timer()
        return self.calculate(gp_sp, db)

    @classmethod
    def plot_x_extent(cls):
        return None

    @classmethod
    def plot_x0(cls):
        return 0

    @classmethod
    def plot_xdelta(cls):
        return 1.0

    @classmethod
    def plot_xlabel(cls):
        return None

    @classmethod
    def plot_ylabel(cls):
        return None

    @classmethod
    def plot_yrange(cls):
        return None

    @classmethod
    def plot_xlog(cls):
        return True

    @classmethod
    def plot_ylog(cls):
        return True

    @classmethod
    def plot_clabel(cls):
        return None


class TimeChunkedProperty(HaloProperties):
    nbins = 1000
    tmax_Gyr = 20.0
    minimum_store_Gyr = 1.0

    @property
    def delta_t(self):
        return self.tmax_Gyr/self.nbins

    @classmethod
    def bin_index(self, time):
        index = int(self.nbins*time/self.tmax_Gyr)
        if index<0:
            index = 0
        return index

    @classmethod
    def store_slice(self, time):
        return slice(self.bin_index(time-self.minimum_store_Gyr), self.bin_index(time))

    @classmethod
    def reassemble(cls, halo, name=None):
        if name is None:
            name = cls.name()

        halo = halo.halo
        t, stack = halo.reverse_property_cascade("t",name,raw=True)

        t = t[::-1]
        stack = stack[::-1]

        final = np.zeros(cls.bin_index(t[-1]))
        for t_i, hist_i in zip(t,stack):
            end = cls.bin_index(t_i)
            start = end - len(hist_i)
            final[start:end] = hist_i

        return final

    @classmethod
    def plot_xdelta(cls):
        return cls.tmax_Gyr/cls.nbins

    @classmethod
    def plot_xlog(cls):
        return False

    @classmethod
    def plot_ylog(cls):
        return False


class LiveHaloProperties(HaloProperties):
    @classmethod
    def requires_simdata(self):
        return False

    def calculate(self, _, db_halo):
        return self.live_calculate(db_halo, [None]*100)


class ProxyHalo(object):

    """Used to return pointers to halos within this snapshot to the database"""

    def __init__(self, value):
        self.value = value

    def __int__(self):
        return int(self.value)



##############################################################################
# UTILITY FUNCTIONS
##############################################################################

def all_property_classes():
    """Return list of all classes derived from HaloProperties"""

    x = HaloProperties.__subclasses__()
    for c in x :
        for s in c.__subclasses__():
            x.append(s)
    return x



def _check_class_provided_name(name):
    if "(" in name or ")" in name:
        raise ValueError, "Property names must not include brackets"

def all_properties():
    """Return list of all properties which can be calculated using
    classes derived from HaloProperties"""
    classes = all_property_classes()
    pr = []
    for c in classes:
        i = c()
        name = i.name()
        if type(name) == str:
            _check_class_provided_name(name)
            pr.append(name)
        else:
            for name_j in name:
                _check_class_provided_name(name_j)
                pr.append(name_j)

    return pr


def providing_class(property_name, silent_fail=False):
    """Return providing class for given property name"""
    classes = all_property_classes()
    property_name = property_name.lower().split("(")[0]
    for c in classes:
        name = c.name()
        if type(name) != str:
            for name_j in name:
                if name_j.lower() == property_name:
                    return c
        elif name.lower() == property_name:
            return c
    if silent_fail:
        return None
    raise NameError, "No providing class for property " + property_name


def providing_classes(property_name_list, silent_fail=False):
    """Return providing classes for given list of property names"""
    classes = []
    for property_name in property_name_list:
        cl = providing_class(property_name, silent_fail)
        if cl not in classes and cl != None:
            classes.append(cl)

    return classes

def _make_numeric_if_possible(s):
    if "." in s:
        try:
            return float(s)
        except ValueError:
            return s
    else:
        try:
            return int(s)
        except ValueError:
            return s

def _process_numerical_value(s,l,t):
    if "." in t[0] or "e" in t[0] or "E" in t[0]:
        return float(t[0])
    else:
        return int(t[0])

def _parse_property_name(name):
    property_name = pp.Word(pp.alphanums+"_")
    property_name_with_params = pp.Forward()
    numerical_value = pp.Regex(r'-?\d+(\.\d*)?([eE]\d+)?').setParseAction(_process_numerical_value)
    value_or_property_name = pp.Group(numerical_value | property_name_with_params)
    parameters = pp.Literal("(").suppress()+pp.Optional(value_or_property_name+pp.ZeroOrMore(pp.Literal(",").suppress()+value_or_property_name))+pp.Literal(")").suppress()
    property_name_with_params << property_name+pp.Optional(parameters)
    property_complete = pp.stringStart()+property_name_with_params+pp.stringEnd()
    return property_complete.parseString(name)

def _regenerate_name(parsed):
    if not isinstance(parsed[0],str):
        return parsed[0]
    name = parsed[0]
    if len(parsed)>1:
        name+="("
        name+=_regenerate_name(parsed[1])
        for other in parsed[2:]:
            name+=","+_regenerate_name(other)
        name+=")"
    return name

def instantiate_classes(property_name_list, silent_fail=False):
    instances = []
    classes = []
    for property_identifier in property_name_list:
        property_parsed = _parse_property_name(property_identifier)
        cl = providing_class(property_parsed[0], silent_fail)
        if cl not in classes and cl != None:
            vals = [_regenerate_name(x) for x in property_parsed[1:]]
            instances.append(cl(*vals))
            classes.append(cl)

    return instances

def instantiate_class(property_name, silent_fail=False):
    instance = instantiate_classes([property_name],silent_fail)
    if len(instance)==0:
        return None
    else:
        return instance[0]

def get_required_properties(property_name):
    return providing_class(property_name).requires_property()

def live_calculate(property_name, db_halo, *args, **kwargs):
    inputs_names = kwargs.pop('names',[None]*100)
    C = providing_class(property_name)
    I = C(*args)
    names = I.name()
    if hasattr(I, 'live_calculate'):
        # new-style context-aware calculation
        results = I.live_calculate(db_halo, inputs_names)
    else:
        # old-style calculation that happens to have no simulation data loaded
        results = I.calculate(None, db_halo)
    if not isinstance(names, str):
        results = results[names.index(property_name)]
    return results





from . import basic, potential, shape, dynamics, profile, flows, images, isolated, subhalo, BH, sfr, dust

