#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import print_function

import sys

def run_importer(argv):
    from tangos import parallel_tasks, core
    from tangos.tools.property_importer import PropertyImporter
    importer = PropertyImporter()
    importer.parse_command_line(argv)
    parallel_tasks.launch(importer.run_calculation_loop, 2, [])

def main():
    print("""
       The 'tangos_import_from_ahf' command line is deprecated in favour of 'tangos property-import'.
       'tangos_import_from_ahf' may be removed in future versions.
       """)
    run_importer(sys.argv[1:])

