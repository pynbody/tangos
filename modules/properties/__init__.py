import numpy as np
import math
import pynbody
import time
import inspect


class HaloProperties(object):

    def requires_array(self):
        """Returns a list of loaded arrays required to
        calculate this property"""
        return []

    def requires_simdata(self):
        """If this returns false, the class can do its
        calculation without any raw simulation data loaded
        (i.e. derived from other properties)"""
        return True

    def name(self):
        """Returns either the name or a list of names of
        properties that will be calculated by this class"""
        return "undefined"

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
        return ["SSC"]

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
            name = cls().name()

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


class ProxyHalo(object):

    """Used to return pointers to halos within this snapshot to the database"""

    def __init__(self, value):
        self.value = value

    def __int__(self):
        return int(self.value)



from . import basic, potential, shape, dynamics, profile, flows, images, isolated, subhalo, BH, sfr, dust


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



def all_properties():
    """Return list of all properties which can be calculated using
    classes derived from HaloProperties"""
    classes = all_property_classes()
    pr = []
    for c in classes:
        i = c()
        name = i.name()
        if type(name) == str:
            pr.append(name)
        else:
            for name_j in name:
                pr.append(name_j)

    return pr


def providing_class(property_name, silent_fail=False):
    """Return providing class for given property name"""
    classes = all_property_classes()
    property_name = property_name.lower().split("[")[0]
    for c in classes:
        i = c()
        name = i.name()
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

def instantiate_classes(property_name_list, silent_fail=False):
    instances = []
    classes = []
    for property_name in property_name_list:
        cl = providing_class(property_name, silent_fail)
        if cl not in classes and cl != None:
            if "[" in property_name:
                args = property_name.split("[")[1][:-1]
                assert "]" not in args
                vals = args.split(",")
                vals = [float(v) for v in vals]
            else:
                vals = []

            instances.append(cl(*vals))
            classes.append(cl)

    return instances


def get_dependent_classes(for_class):

    out_classes = []
    provides = for_class().name()
    if type(provides) == str:
        provides = [provides]

    for c in all_property_classes():
        needs = c().requires_property()
        if any([p in needs for p in provides]):
            out_classes.append(c)
            sub_dep = get_dependent_classes(c)
            for s in sub_dep:
                if s not in out_classes:
                    out_classes.append(s)

    return out_classes


def resolve_dependencies(existing_properties, classes):
    """Return ordered list of classes to run to calculate properties in
    specified list of classes, given the dictionary of existing properties.

    This automagically (1) orders classes such that dependencies are
                           available when required

                       (2) adds classes if they calculate a required
                           dependency"""

    ordered_classes = []

    for c in classes:
        depend = [
            d for d in c().requires_property() if d not in existing_properties]
        for d in depend:
            cl = providing_class(d)
            if cl == None:
                raise RuntimeError(
                    "Dependencies cannot be resolved -- no providing class for property " + d)
            classes = resolve_dependencies(existing_properties, [cl])
            classes.reverse()
            for cl in classes:
                # bring to front
                if cl in ordered_classes:
                    del ordered_classes[ordered_classes.index(cl)]
                ordered_classes = [cl] + ordered_classes

        if c not in ordered_classes:
            ordered_classes.append(c)

    return ordered_classes


def gc_codelib(meta_properties, code_library):

    valid_hash = []
    delete_hash = []
    for i in meta_properties:
        for k in i:
            try:
                if i[k]['code'] not in valid_hash:
                    valid_hash.append(i[k]['code'])
            except KeyError:
                pass

    print "Code library: ", len(code_library), " items"

    for k in code_library:
        code = code_library[k].split("\n")
        if k not in valid_hash:
            print "(del) ", code[0], "(l", len(code), ")"
            delete_hash.append(k)
        else:
            print "      ", code_library[k].split("\n")[0], "(l", len(code), ")"

    print "Removing ", len(delete_hash), " items"
    for d in delete_hash:
        del code_library[d]


def codehash(code):
    import hashlib
    import re
    return hashlib.sha1(re.sub("\s+", "", code)).hexdigest()


def safe_write(fname, p_array, code_library, meta_properties):
    import cPickle as pickle
    try:
        pickle.dump(p_array, file(fname + ".halo_properties", "w"))
        pickle.dump((code_library, meta_properties), file(
            fname + ".halo_properties.meta", "w"))
    except KeyboardInterrupt:
        import sys
        print "(please wait, writing files)",
        sys.stdout.flush()
        safe_write(fname, p_array, code_library, meta_properties)
        raise KeyboardInterrupt


code_lib = {}


def ld_dat(fname):
    import cPickle as pickle
    dat = pickle.load(file(fname))
    try:
        meta = pickle.load(file(fname + ".meta"))
        for d, m in zip(dat, meta[1]):
            infl = {}
            for t in m:
                # magic inflation of DLA_blahblah_* type keys
                if t[-1] == "*":
                    # find matching data keys
                    for k in d:
                        if k[:len(t) - 1] == t[:-1] and "_" not in k[len(t):]:
                            infl[k] = m[t]
            m.update(infl)
            d["**"] = m

        code_lib.update(meta[0])
    except IOError:
        pass
    return dat


def info(dataset, propertyname):
    """Using property metadata inflated using ld_dat, writes useful
    information about the specified property in the given dataset"""
    import time

    reverse_d = {}

    for k in dataset:
        if k.has_key("**") and k.has_key(propertyname) or propertyname[-1] == "*":
            try:
                st = k["**"][propertyname]["code"]

            except KeyError:
                pass

            if reverse_d.has_key(st):
                if k["legend"] not in reverse_d[st]:
                    reverse_d[st].append(k["legend"])
            else:
                reverse_d[st] = [k["legend"]]

    xkid = None
    for k in reverse_d:
        print "*" * 51
        if k == None:
            print "Unknown is used by "
        else:
            print k[:3], "is used by "
        for z in reverse_d[k]:
            st = None
            for q in dataset:
                try:
                    if q["legend"] == z and q["**"][propertyname]["code"] == k:
                        if st == None:
                            st = q["uid"]
                            ifo = q["**"][propertyname]["host"] + \
                                time.strftime(
                                    " %D %H:%M", q["**"][propertyname]["calculated"])
                        stx = q["uid"]
                    else:
                        if st != None:
                            print st, "->", stx, ":", ifo
                            st = None
                except KeyError:
                    pass

            if st != None:
                print st, "->", stx, ":", ifo

        if xkid != None:
            print "- " * 26
            if k != "?" and k != None:
                print "copy paste diff command : properties.cdiff('" + xkid[:3] + "','" + k[:3] + "')"
        else:
            if k != "?" and k != None:
                xkid = k


def expand_hash(snippet):
    candidates = [i for i in code_lib if type(
        i) == str and snippet == i[:len(snippet)]]
    if len(candidates) == 0:
        raise RuntimeError, "No match for snippet"
    elif len(candidates) > 1:
        raise RuntimeError, "Multiple matches for snippet"
    else:
        return candidates[0]


def cdiff(i1, i2):
    import difflib

    cd1 = code_lib[expand_hash(i1)].splitlines(1)
    cd2 = code_lib[expand_hash(i2)].splitlines(1)

    d = difflib.Differ()
    result = list(d.compare(cd1, cd2))
    r = [q for q in result if q[0] != " "]
    print "".join(r)


def interpret_file_arguments(argv):
    """Smart interpretation of what files are to be processed by
    a script. Returns a list of files and the remaining command line."""
    import os
    import glob
    files = []
    out = []

    def _add_files(a, file_list):
        """Returns true if some files were matched off the candidate a"""
        if os.path.exists(a):
            if os.path.exists(a + ".amiga.grp") or os.path.exists(a + ".amiga.grp.gz"):
                file_list.append(a)
            elif os.path.isdir(a):
                return np.sometrue([_add_files(f, file_list) for f in glob.glob(a + "/*")])
            else:
                return False
        else:
            return False
        return True

    for a in argv:
        if not _add_files(a, files):
            out.append(a)

    return files, out


def match_properties(k, kto, keys):
    kx = k.split("*")

    ktox = kto.split("*")

    assert(len(kx) == len(ktox))
    if len(kx) == 1:
        if k in keys:
            return [(k, kto)]
        else:
            return []
    else:
        output = []
        for cand in keys:
            targ_cand = cand
            for f, r in zip(kx, ktox):
                if f != "":
                    targ_cand = r.join(targ_cand.split(f))
            if cand != targ_cand:
                output.append((cand, targ_cand))
        return output


def mv(sname, dname, prop, metaprop=None, interactive=False, cp=False):

    import terminalcontroller as t
    warned = False

    for p in prop:
        repl = match_properties(sname, dname, p.keys())
        if len(repl) > 0 and not warned:
            if interactive:
                t.heading("CONFIRM...")
            for a, b in repl:
                if b[:5] != "trash":
                    print a, "->", b
                else:
                    print t.term.RED + "trash " + t.term.NORMAL + a

            if interactive:
                t.heading("TYPE YES TO CONTINUE, NO TO QUIT...")
                c = raw_input()
                if c != "YES":
                    raise RuntimeError, "Quit"
            warned = True
        for a, b in repl:
            if p.has_key(b):
                raise RuntimeError, "Name clash in target"
            if b[:5] != "trash":
                p[b] = p[a]
            if not cp:
                del p[a]

    if metaprop != None:
        warned = False
        for mp in metaprop:
            repl = match_properties(sname, dname, mp.keys())
            if not warned and len(repl) > 0:
                t.heading("In MetaProperties it looks like this:")
                for a, b in repl:
                    if b[:5] != "trash":
                        print a, "->", b
                    else:
                        print t.term.RED + "trash " + t.term.NORMAL + a
                warned = True
            for a, b in repl:
                if b[:5] != "trash":
                    mp[b] = mp[a]
                if not cp:
                    del mp[a]
