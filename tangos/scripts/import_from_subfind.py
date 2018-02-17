#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function
import argparse

import tangos as db
import tangos.core.timestep
import pynbody

def resolve_units(value):
    if(pynbody.units.is_unit(value)):
        return float(value)
    else:
        return value

def process_halos_or_groups(halos_or_groups, use_properties, pynbody_prefix):
    session = db.core.get_default_session()
    objects = []

    first = halos_or_groups.first()
    if first:
        ts_keep_alive = first.timestep.load()

    for h in halos_or_groups:

        pynbody_properties = h.load(mode='subfind_properties')

        for k in use_properties:
            if pynbody_prefix + k in pynbody_properties:
                data = resolve_units(pynbody_properties[pynbody_prefix + k])
                k_id = tangos.core.get_or_create_dictionary_item(session, k)
                objects.append(tangos.core.HaloProperty(h, k_id, data))

        if "sub_parent" in pynbody_properties:
            targ = db.core.get_default_session().query(db.core.halo.Group).filter_by(
                timestep_id=h.timestep_id,
                finder_id=int(pynbody_properties['sub_parent'])).first()
            if targ is not None:
                parent_id = tangos.core.get_or_create_dictionary_item(session, "parent")
                child_id = tangos.core.get_or_create_dictionary_item(session, "child")
                objects.append(tangos.core.HaloLink(h, targ, parent_id))
                objects.append(tangos.core.HaloLink(targ, h, child_id))

    session.add_all(objects)
    return len(objects)

def main():

    parser = argparse.ArgumentParser()
    db.supplement_argparser(parser)

    parser.add_argument('--sims','--for', action='store', nargs='*',
                                metavar='simulation_name',
                                help='Specify a simulation (or multiple simulations) to run on')

    parser.add_argument('properties', action='store', nargs='*',
                            help="The names of the halo/group properties to import from Subfind, or leave blank to import all data")

    parser.add_argument('--backwards', action='store_true',
                            help='Process low-z timesteps first')

    args = parser.parse_args()

    db.process_options(args)


    base_sim = db.sim_query_from_name_list(args.sims)

    all_properties = ["CM","HalfMassRad","VMax","VMaxRad","mass","pos","spin","vel","veldisp", # halos, i.e. subfind subs
                      "mass","mcrit_200","mmean_200","mtop_200","rcrit_200","rmean_200","rtop_200", # groups
                      ]

    use_properties = args.properties
    if len(use_properties)==0:
        use_properties=all_properties


    with db.get_default_session().no_autoflush: # for performance
        for x in base_sim:
            if not isinstance(x.get_output_handler(),
                              tangos.input_handlers.pynbody.GadgetSubfindInputHandler):
                raise ValueError("import_from_subfind requires the handler to be SubfindInputHandler")
            timesteps = db.core.get_default_session().query(tangos.core.timestep.TimeStep).filter_by(
                simulation_id=x.id, available=True).order_by(tangos.core.timestep.TimeStep.redshift.desc()).all()

            if args.backwards:
                timesteps = timesteps[::-1]


            for ts in timesteps:
                tangos.log.logger.info("Processing timestep %r", ts)

                num_added = process_halos_or_groups(ts.halos, use_properties, "sub_")

                tangos.log.logger.info("Added %d halo properties",num_added)

                num_added = process_halos_or_groups(ts.groups, use_properties, "")
                tangos.log.logger.info("Added %d group properties", num_added)

                db.core.get_default_session().commit()
