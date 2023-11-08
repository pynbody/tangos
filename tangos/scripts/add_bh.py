#!/usr/bin/env python

import sys

from tangos import core, parallel_tasks
from tangos.parallel_tasks import database
from tangos.tools.changa_bh_importer import ChangaBHImporter


def run_dbwriter(argv):
    database.synchronize_creator_object()
    writer = ChangaBHImporter()
    writer.parse_command_line(argv)
    writer.run_calculation_loop()


def main():
    print("""
    The 'tangos_add_bh' command line is deprecated in favour of 'tangos import-changa-bh'.
    'tangos_add_bh' may be removed in future versions.
    """)
    parallel_tasks.launch(run_dbwriter,  [sys.argv[1:]])
