#!/usr/bin/env python2.7
"""
magic_amiga.py by Andrew Pontzen

This script is supposed to make it much easier to AMIGA all your gasoline output.
Before starting, you probably need to change some of the lines in localset.py
which contains the command line instructions to AMIGA etc."""



import re
from tangos.parallel_tasks import distributed, launch
from tangos.simulation_output_handlers.finding import *


identifier = "magic_amiga.py v0.0"

import glob
import os
import math
import sys
import time

def run():
    print "Scanning..."
    global only_idl, no_idl

    if "idl" in sys.argv:
        only_idl = True
        del sys.argv[sys.argv.index("idl")]

    par = False

    if "par" in sys.argv:
        par = True
        index = sys.argv.index("par")
        del sys.argv[index]
        del sys.argv[index]

    restr = None
    if "only" in sys.argv:
        restr = sys.argv[sys.argv.index("only") + 1]
        del sys.argv[sys.argv.index("only"):sys.argv.index("only") + 2]

    res = None
    if "res" in sys.argv:
        res = int(sys.argv[sys.argv.index("res") + 1])
        del sys.argv[sys.argv.index("res"):sys.argv.index("res") + 2]

    no_idl = False
    if "no_idl" in sys.argv or "no-idl" in sys.argv:
        no_idl = True
        try:
            sys.argv.remove("no_idl")
        except:
            pass
        try:
            sys.argv.remove("no-idl")
        except:
            pass

    if not only_idl:
        outputs = find()

        outputs_ahf = ahf_dejunk(find("AHF_halos"))

        to_amiga = outputs - outputs_ahf

        if restr is not None:
            to_amiga = [x for x in to_amiga if restr in x]

        to_amiga = sorted(to_amiga)

        print "Found", len(outputs), "outputs"
        print len(outputs_ahf), "with AHF already run, so..."
        print "**** RUNNING AMIGA ON", len(to_amiga), "OUTPUTS ****"
        if len(to_amiga) != 0:
            print "**** Here is the list of outputs which I'm going to AMIGA ... ****"
            print to_amiga
            print "**** Starting AMIGA runs... ****"

        if par:
            to_amiga = distributed(to_amiga)

        for i in to_amiga:
            print "  Processing", i

            path = ("/".join(i.split("/")[:-1]))
            if path == "":
                path = "."

            if newstyle_amiga:
                amiga_param_file = i + ".AHF_param"
                pf = file(amiga_param_file, "w")

                tpars = info_from_params(get_param_file(i), None)

                lgrid = 65536  # for 768 run

                if res is None:
                    print "res from ",i
                    try:
                        res = int(
                            re.findall("([0-9][0-9][0-9]+)(g|.0|.tip|.c)", i.split("/")[-1])[0][0])
                    except (IndexError, ValueError):
                        if res is None:
                            raise RuntimeError, "Cannot work out run resolution from filename"

                print "res=",res
                base = 768
                if "cosmo6" in i:
                    base = 192

                try:
                    import pynbody
                    f = pynbody.load(i)
                    scalefac = f.properties['a']
                    if scalefac < 0.1:
                        scalefac = 0.1
                except ImportError:
                    scalefac = 1.0

                lgrid *= 2 ** int(math.log(scalefac *
                                           res / base) / math.log(2))
                print res, "->", lgrid

                print >>pf, amiga_params % tuple([i, i, lgrid] + tpars)
                del pf

                start_run = amiga_param_file

            else:
                tipsy_info_file = path + "/tipsy.info"

                if not os.path.isfile(tipsy_info_file):
                    print "   > creating ", tipsy_info_file
                    pfile = get_param_file(i)
                    print "   > param file is ", pfile
                    info_from_params(pfile, tipsy_info_file)

                tname = i.split("/")[-1]

                start_run = i + ".AHF_startrun"
                srf = file(start_run, "w")
                print >>srf, tname + " 90 1"
                print >>srf, tname
                print >>srf, amiga_params
                print >>srf, "# Auto-created by " + identifier

                del srf  # close file efore launching amiga

            try:
                # basic check that file is readable
                f = file(i)
                del f

                if os.system(amiga_command % (path, start_run.split("/")[-1])) != 0 and not ignore_error:
                    raise RuntimeError, "Command exited with error"

                # the following so that a double ctrl^c definitely exits even if
                # return vals from os.system not playing ball
                time.sleep(1)

            except IOError:
                ex = "* FAIL on reading " + i + " *"
                print "*" * len(ex)
                print ex
                print "*" * len(ex)

        print "<>" * 20

    if no_idl:
        sys.exit(0)

    # Re-assess the situation
    outputs_ahf = ahf_dejunk(find("AHF_halos"))
    outputs_alyson = find("amiga.grp")

    # print "Found",len(outputs_ahf),"outputs with AMIGA files"
    # print "Found",len(outputs_alyson),"with Alyson's magic"
    to_alyson = outputs_ahf - outputs_alyson
    to_alyson = sorted(to_alyson)
    print "**** RUNNING ALYSON'S SCRIPT ON", len(to_alyson), "OUTPUTS ****"

    if len(to_alyson) != 0:
        print "**** Here is the list of things I'm running Alyson's script on ****"
        print to_alyson
        print "**** Starting Alyson's script runs... ****"

    # HEALTH WARNING -- assumes the value of h0
    h0 = 0.73


    if par:
        to_alyson = distributed(to_alyson)

    for i in to_alyson:
        h0 = info_from_params(get_param_file(i), None, return_hubble=True)
        print "  processing", i, "h0=",h0
        path = ("/".join(i.split("/")[:-1]))
        if path == "":
            path = "."

        amiga_in = glob.glob(i + "*AHF_halos")[0]  # guaranteed to exist now
        zspec = ahf_getjunk(amiga_in)
        start_run = i + ".IDL_startrun"
        srf = file(start_run, "w")
        tipsy_if = file(path + "/tipsy.info")
        tipsy_if.readline()
        tipsy_if.readline()
        dunit = float(tipsy_if.readline().strip().split(" ")[0]) * 1000
        vunit = float(tipsy_if.readline().strip().split(" ")[0])
        munit = float(tipsy_if.readline().strip().split(" ")[0]) / h0

        print >>srf, idl_preamble
        if idl_pass_z:
            print >>srf, idl_proc_name + ",'" + i + "','" + zspec + \
                "',", dunit, ",munit=", munit, ",vunit=", vunit, ",h0=", h0
        else:
            print >>srf, idl_proc_name + ",'" + i + \
                "',boxsize=%e,munit=%e,vunit=%e,h0=%f" % (
                    dunit, munit, vunit, h0)
        print >>srf, "exit"
        print >>srf, "; These are the commands which were passed to IDL by " + \
            identifier
        del srf

        if os.system(idl_command + " " + start_run) != 0 and not ignore_idl_error:
            raise RuntimeError, "Error reported from IDL"


if __name__ == "__main__":
    
    par = False

    if "par" in sys.argv:
        par = True
        num_proc = int(sys.argv[sys.argv.index("par")+1])

    if par:
        launch(run,num_proc)
    else:
        run()

