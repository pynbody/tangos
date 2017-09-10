"""Configuration module for tangos

Rather than change anything directly in this file, you can create a config_local.py with the variable you
want to override and it will automatically take precedence.
"""

import os
import sys

home = os.environ['HOME']

if sys.platform=='darwin' :
    home+='/Science/'

db = os.environ.get("TANGOS_DB_CONNECTION", home+"/tangos_data.db")
base = os.environ.get("TANGOS_SIMULATION_FOLDER", home+"/")

default_fileset_handler_class = "pynbody.ChangaOutputSetHandler"

num_multihops_max_default = 100

min_halo_particles = 1000


file_ignore_pattern = []


try:
    from .config_local import *
except:
    pass