#!/usr/bin/env python2.7

from __future__ import absolute_import
import sys

def run_dbwriter(argv):
    from tangos import parallel_tasks, core
    from tangos.tools.property_writer import PropertyWriter
    writer = PropertyWriter()
    writer.parse_command_line(argv)
    parallel_tasks.launch(writer.run_calculation_loop, 2, [])

def main():
    print("""
    The 'tangos_writer' command line is deprecated in favour of 'tangos write'.
    'tangos_writer' may be removed in future versions.
    """)
    run_dbwriter(sys.argv[1:])


