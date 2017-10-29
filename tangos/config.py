"""Configuration module for tangos

Rather than change anything directly in this file, you can create a config_local.py with the variable you
want to override and it will automatically take precedence.
"""

from __future__ import absolute_import
import os
import sys

home = os.environ['HOME']

if sys.platform=='darwin' :
    home+='/Science/'

db = os.environ.get("TANGOS_DB_CONNECTION", home+"/tangos_data.db")
base = os.environ.get("TANGOS_SIMULATION_FOLDER", home+"/")

default_fileset_handler_class = "pynbody.PynbodyOutputSetHandler"

num_multihops_max_default = 100
# the maximum number of links to follow when searching for related halos

default_linking_threshold = 0.005
# the percentage of particles in common between two objects before the database even bothers to store the relationship

min_halo_particles = 1000
# the minimum number of particles needed in an object before the database bothers to store it

default_backend = 'null'
# the default paralellism backend. Set e.g. to mpi4py to avoid having to pass --backend mpi4py to all parallel runs.


property_modules = os.environ.get("TANGOS_PROPERTY_MODULES","") # names of property modules to import
property_modules = property_modules.split(",") + ["tangos_nbodyshop_properties"]
property_modules = map(str.strip, property_modules)

file_ignore_pattern = []

max_traverse_depth = 3

# merger tree building thinning criteria
mergertree_min_fractional_weight = 0.02 # as a fraction of the weight of the strongest link from each halo
mergertree_min_fractional_NDM = 0 # as a fraction of the most massive halo at each timestep - set to zero for no thinning
mergertree_max_nhalos = 30 # maximum number of halos per step - discard the least massive ones
mergertree_timeout = 15.0 # seconds before abandoning the construction of a merger tree in the web interface


try:
    from .config_local import *
except:
    pass