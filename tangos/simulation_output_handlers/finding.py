import glob
import math
import fnmatch

from ..config  import *


def find(extension=None, mtd=None, ignore=None, basename=""):
    if mtd == None:
        mtd = max_traverse_depth
    if ignore == None:
        ignore = file_ignore_pattern
    out = []

    if extension is not None:
        for d in xrange(mtd + 1):
            out += glob.glob(basename + ("*/" * d) + "*." + extension)

        out = [f[:-(len(extension) + 1)] for f in out]

        out = filter(
            lambda f: not any([fnmatch.fnmatch(f, ipat) for ipat in ignore]), out)
    else:
        for d in xrange(mtd + 1):
            out += glob.glob(basename + ("*/" * d) + "*.00???")
            out += glob.glob(basename + ("*/" * d) + "*.00????")

    return set(out)


def get_param_file(output_file):
    """Work out the param file corresponding to the
    specified output"""

    q = "/".join(output_file.split("/")[:-1])
    if len(q) != 0:
        path = "/".join(output_file.split("/")[:-1]) + "/"
    else:
        path = ""

    candidates = glob.glob(path + "*.param")

    if len(candidates) == 0:
        candidates = glob.glob(path + "../*.param")

    if len(candidates) == 0:
        raise RuntimeError, "No .param file in " + path + \
            " (or parent) -- please supply or create tipsy.info manually"

    candidates = filter(lambda x: "direct" not in x and "mpeg_encode" not in x,
                        candidates)

    if len(candidates) > 1:
        raise RuntimeError, "Can't resolve ambiguity -- too many param files matching " + \
            path

    return candidates[0]


def param_file_to_dict(param_file):
    f = file(param_file)
    out = {}

    for line in f:
        try:
            s = line.split()
            if s[1] == "=" and "#" not in s[0]:
                key = s[0]
                v = s[2]

                if key[0] == "d":
                    v = float(v)
                elif key[0] == "i" or key[0] == "n" or key[0] == "b":
                    v = int(v)

                out[key] = v
        except (IndexError, ValueError):
            pass
    return out


def info_from_params(param_file, tipsy_info_file, return_hubble=False):
    f = file(param_file)

    munit = dunit = hub = None
    for line in f:
        try:
            s = line.split()
            if s[0] == "dMsolUnit":
                munit = float(s[2])
            elif s[0] == "dKpcUnit":
                dunit = float(s[2])
            elif s[0] == "dHubble0":
                hub = float(s[2])
            elif s[0] == "dOmega0":
                om_m0 = s[2]
            elif s[0] == "dLambda":
                om_lam0 = s[2]

        except IndexError, ValueError:
            pass

    if munit == None or dunit == None or hub == None:
        raise RuntimeError("Can't find all parameters required in .param file")

    denunit = munit / dunit ** 3
    # see original param2units.py for explanation of this factor
    velunit = 8.0285 * math.sqrt(6.67300e-8 * denunit) * dunit

    hub *= 10. * velunit / dunit

    if return_hubble:
        return hub

    if tipsy_info_file is not None:
        tu = file(tipsy_info_file, "w")
        print >>tu, om_m0
        print >>tu, om_lam0
        print >>tu, 1.e-3 * dunit * hub

        print >>tu, velunit
        print >>tu, munit * hub
        print >>tu, " "
        print >>tu, "# Auto-created from " + param_file + " by " + identifier
    else:
        return [1.e-3 * dunit * hub, munit * hub, velunit, 0.025, om_m0, om_lam0]


def ahf_dejunk(t):
    """Remove the redshift etc junk information on AHF file outputs
    to get a clear comparative"""
    if type(t) == list or type(t) == set:
        return set([ahf_dejunk(x) for x in t])

    cleaned = ".z".join(t.split(".z")[:-1])
    if cleaned[-5:] == ".0000":
        cleaned = cleaned[:-5]

    if cleaned == "":
        cleaned = "z".join(t.split("z")[:-1])

    return cleaned


def ahf_getjunk(t):
    """Get the junky z* spec from an AHF filename for Alyson's script"""

    if "0000.z" in t:
        plop = t.split(".z")[-1]
        return "z" + (".".join(plop.split(".")[:2]))
    else:
        plop = t.split(".z")[-1]
        return "z" + (".").join(plop.split(".")[:2])
        #raise RuntimeError, "Old style AMIGA output, by the looks of things -- sorry, not set up to handle that"




def failmsg(m):
    """Pretty-print a failure message"""
    msg = "* FAIL " + m + " *"
    print "*" * len(msg)
    print msg
    print "*" * len(msg)
