#!/usr/bin/env python2.7

from __future__ import absolute_import
import sys

def run_dbwriter(argv):
    import matplotlib
    matplotlib.use('agg')
    from tangos import parallel_tasks, core
    from tangos.tools.property_writer import PropertyWriter
    writer = PropertyWriter()
    writer.parse_command_line(argv)
    parallel_tasks.launch(writer.run_calculation_loop, 2, [])

def main():
    run_dbwriter(sys.argv[1:])

if __name__ == "__main__":

    run_dbwriter(sys.argv[1:])

