"""Support routines for tracking regions/sets of particles through a simulation.

Tracked regions consist of a list of particles (stored in the database) alongside a set of Tracker
objects attached to each timestep which give access to the data associated with those particles.
For example one might be interested in tracing where the ISM of a galaxy came from, or where it ends
up. Tracker objects enable this. 

See https://pynbody.github.io/tangos/tracking.html or docs/tracking.md for more information and an example.
"""

from __future__ import absolute_import
from __future__ import print_function
from sqlalchemy.orm import Session
import numpy as np
from .core import get_or_create_dictionary_item, Halo, HaloLink, TrackData
from . import query
import six
import tangos.parallel_tasks as parallel_tasks
from six.moves import zip

def get_trackers(sim):
    """Get all the trackers and their numbers for a given simulation. 
    
    Used internally by tracking code in tangos.
 
    :rval trackers, numbers
    trackers - a list of Tracker objects, which contain the particle IDs for each tracker
    numbers - a numpy array of the Tracker's object numbers
    
    Object numbers N can be used to retrieve the tracked particles at a given timestep, using
    
    tangos.get_object("sim/timestep/tracker_N")
    """
    
    trackers = sim.trackers.all()
    nums = [tx.halo_number for tx in trackers]
    return trackers, np.array(nums)

def get_tracker_links(session, relation):
    """Get all links with a given relation in the simulation.
    
    Used internally by tracking code.
    """

    links = session.query(HaloLink).filter_by(relation=relation).all()
    idf = [tl.halo_from_id for tl in links]
    idt = [tl.halo_to_id for tl in links]
    return links, np.array(idf), np.array(idt)

def new(for_simulation, using_particles):
    """Create a new tracked region for the specific simulation using the specified particles.
    
    This routine is not strictly pynbody-specific in its implementation but it is built 
    with the pynbody backend in mind."""
    
    if isinstance(for_simulation, six.string_types):
        for_simulation = query.get_simulation(for_simulation)
    tracker = TrackData(for_simulation)
    use_iord = 'iord' in using_particles.loadable_keys()
    tracker.select(using_particles, use_iord)
    session = Session.object_session(for_simulation)
    session.add(tracker)
    tracker.create_objects()
    tracker.create_links()
    session.commit()
    return tracker.halo_number
