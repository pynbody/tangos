#!/usr/bin/env python

import glob
import os
import sys
import socket
import getpass
import tempfile

all_steps = False
steps = []
aux_arrays = ["ESNRate", "FeMassFrac", "H2", "HI", "HeI", "HeII", "OxMassFrac",
              "amiga.grp", "*AHF*", "amiga.stat", "amiga.gtp", "coolontime", "den",
              "igasorder", "iord", "massform", "timeform", "rung"]


def get_bases(hint, steps):

    bases = glob.glob(hint + "/*.00???") + glob.glob(hint + "/*.00????")

    files = []

    for b in bases:

        if not all_steps and not any([b.endswith(str(X).rjust(4, "0")) for X in steps]):
            continue

        if os.path.isdir(b) or os.path.islink(b):

            X = b + "/" + os.path.basename(b)
        else:
            X = b

        files.append(X)

    return files


def expand_base(basename):
    files = [basename]

    for ext in aux_arrays:
        candidate = basename + "." + ext
        if os.path.isfile(candidate):
            files.append(candidate)
        else:
            p = glob.glob(candidate)
            files+=p

    return files


def syntax():
    print "Syntax: transfer_list.py <paramfile> [<Nstep1> <Nstep2>]"

if len(sys.argv) < 3 or not os.path.isfile(sys.argv[1]):
    syntax()
else:
    steps = [int(x) for x in sys.argv[2:]]

    files = [[sys.argv[1]]]
    logfile = ".".join(sys.argv[1].split(".")[:-1]) + ".log"

    if os.path.isfile(logfile):
        files += [[logfile]]
    else:
        print>>sys.stderr, "Warning: log file not found"

    files += [expand_base(X)
              for X in get_bases(os.path.dirname(sys.argv[1]), steps)]
    files = sum(files, [])

    _, out_file_name = tempfile.mkstemp(text=True)

    out_file = file(out_file_name, "w")

    remote_host = "%s@%s" % (getpass.getuser(), socket.gethostname())

    print>>out_file, "\r\n".join(files)
    out_file.close()


    print>>sys.stderr, "A total of ", len(files), " to transfer."
    print>>sys.stderr, ""
    
    print "To pull from a remote host, use:"
    print " rsync -vzhP --files-from=%s:%s %s:%s ./" % (remote_host, out_file_name,
                                                                  remote_host,
                                                                  os.path.dirname(os.path.abspath(os.path.dirname(sys.argv[1]))))
    
    print "To push to a remote host, use:"
    print " rsync -vzhP --files-from=%s %s [remotehost:/path/]"%(out_file_name, os.path.dirname(os.path.abspath(os.path.dirname(sys.argv[1]))))
