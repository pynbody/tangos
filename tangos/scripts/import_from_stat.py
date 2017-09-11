#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function
import argparse

import tangos as db
import tangos.core.timestep
from tangos.simulation_output_handlers import halo_stat_files as hsf
from tangos.util.terminalcontroller import term


def main():

    parser = argparse.ArgumentParser()
    db.supplement_argparser(parser)

    parser.add_argument('--sims','--for', action='store', nargs='*',
                                metavar='simulation_name',
                                help='Specify a simulation (or multiple simulations) to run on')

    parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the halo properties to import from the AHF_halos file")

    parser.add_argument('--backwards', action='store_true',
                            help='Process low-z timesteps first')

    args = parser.parse_args()

    db.process_options(args)


    base_sim = db.sim_query_from_name_list(args.sims)

    names = args.properties

    for x in base_sim:
        timesteps = db.core.get_default_session().query(tangos.core.timestep.TimeStep).filter_by(
            simulation_id=x.id, available=True).order_by(tangos.core.timestep.TimeStep.redshift.desc()).all()

        if args.backwards:
            timesteps = timesteps[::-1]

        for ts in timesteps:
            print(term.GREEN, "Processing ",ts, term.NORMAL)
            hsf.HaloStatFile(ts).add_halo_properties(*names)
            db.core.get_default_session().commit()
