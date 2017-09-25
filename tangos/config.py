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

default_fileset_handler_class = "pynbody.ChangaOutputSetHandler"

num_multihops_max_default = 100
# the maximum number of links to follow when searching for related halos

default_linking_threshold = 0.005
# the percentage of particles in common between two objects before the database even bothers to store the relationship

min_halo_particles = 1000
# the minimum number of particles needed in an object before the database bothers to store it

default_backend = 'null'
# the default paralellism backend. Set e.g. to mpi4py to avoid having to pass --backend mpi4py to all parallel runs.

file_ignore_pattern = []

max_traverse_depth = 3

try:
    from .config_local import *
except:
    pass
