#!/usr/bin/env python2.7


from __future__ import absolute_import
from tangos import core
from tangos import parallel_tasks
from tangos.parallel_tasks import database
from tangos.tools.crosslink import TimeLinker
import sys


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

