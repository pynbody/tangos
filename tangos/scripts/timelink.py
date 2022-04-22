#!/usr/bin/env python2.7


import sys

from tangos import parallel_tasks
from tangos.tools.crosslink import TimeLinker


def run_dbwriter(argv):
    parallel_tasks.database.synchronize_creator_object()
    writer = TimeLinker()
    writer.parse_command_line(argv)
    writer.run_calculation_loop()

def main():
    print("""
    The 'tangos_timelink' command line is deprecated in favour of 'tangos link'.
    'tangos_timelink' may be removed in future versions.
    """)
    parallel_tasks.launch(run_dbwriter, 2, [sys.argv[1:]])
