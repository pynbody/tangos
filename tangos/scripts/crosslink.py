#!/usr/bin/env python2.7


import sys

from tangos import parallel_tasks
from tangos.parallel_tasks import database
from tangos.tools.crosslink import CrossLinker


def run_dbwriter(argv):
    database.synchronize_creator_object()
    writer = CrossLinker()
    writer.parse_command_line(argv)
    writer.run_calculation_loop()


def main():
    print("""
    The 'tangos_crosslink' command line is deprecated in favour of 'tangos crosslink'.
    'tangos_crosslink' may be removed in future versions.
    """)
    parallel_tasks.launch(run_dbwriter, 2, [sys.argv[1:]])
