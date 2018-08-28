"""Configuration module for tangos

Rather than change anything directly in this file, you can create a config_local.py with the variable you
want to override and it will automatically take precedence.
"""

from __future__ import absolute_import
import os

home = os.environ['HOME']
db = os.environ.get("TANGOS_DB_CONNECTION", home+"/tangos_data.db")
base = os.environ.get("TANGOS_SIMULATION_FOLDER", home+"/")

default_fileset_handler_class = "pynbody.PynbodyInputHandler"

num_multihops_max_default = 100
# the maximum number of links to follow when searching for related halos

default_linking_threshold = 0.005
# the percentage of particles in common between two objects before the database bothers to store the relationship

min_halo_particles = 1000
# the minimum number of particles needed in an object before the database bothers to store it

max_num_objects = None
# the maximum number of objects of any one class to store in the database (or None for no limit).
# Normally, the most massive objects will be stored, but this does depend on either the halo finder
# ordering appropriately, or the re-ordering of objects being enabled (which is the default)

default_backend = 'null'
# the default paralellism backend. Set e.g. to mpi4py to avoid having to pass --backend mpi4py to all parallel runs.


# names of property modules to import; default is for backwards compatibility on systems with N-Body-Shop extensions
property_modules = os.environ.get("TANGOS_PROPERTY_MODULES","tangos_nbodyshop_properties")
property_modules = property_modules.split(",")
property_modules = map(str.strip, property_modules)

file_ignore_pattern = []

max_traverse_depth = 3

# merger tree thinning criteria (applied at query time, not at time of writing links)
mergertree_min_fractional_weight = 0.02 # as a fraction of the weight of the strongest link from each halo
mergertree_min_fractional_NDM = 0.01 # as a fraction of the most massive halo at each timestep - set to zero for no thinning
mergertree_max_nhalos = 30 # maximum number of halos per step - discard the least massive ones
mergertree_timeout = 15.0 # seconds before abandoning the construction of a merger tree in the web interface
mergertree_max_hops = 500 # maximum number of timesteps to scan

# On some network file systems, concurrency using sqlite is dodgy to say the least. After committing a transaction
# on one node, and before attempting to open a new transaction on another node, it seems empirically helpful to
# allow a significant time delay. This variable controls that delay.
DEFAULT_SLEEP_BEFORE_ALLOWING_NEXT_LOCK = 1.0
# number of seconds to sleep after a lock is released before reallocating it

try:
    from .config_local import *
except:
    pass
