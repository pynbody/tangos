#!/usr/bin/env python

import sys

from tangos import core, parallel_tasks
from tangos.parallel_tasks import database
from tangos.tools.bh_importer import BHImporter


def run_dbwriter(argv):
    database.synchronize_creator_object()
    writer = BHImporter()
    writer.parse_command_line(argv)
    writer.run_calculation_loop()


def main():
    print("""
    The 'tangos_add_bh' command line is deprecated in favour of 'tangos add-bh'.
    'tangos_add_bh' may be removed in future versions.
    """)
    parallel_tasks.launch(run_dbwriter, 2, [sys.argv[1:]])
