#!/usr/bin/env python


import sys


def run_importer(argv):
    from tangos import core, parallel_tasks
    from tangos.tools.property_importer import PropertyImporter
    importer = PropertyImporter()
    importer.parse_command_line(argv)
    parallel_tasks.launch(importer.run_calculation_loop,  [])

def main():
    print("""
       The 'tangos_import_from_ahf' command line is deprecated in favour of 'tangos property-import'.
       'tangos_import_from_ahf' may be removed in future versions.
       """)
    run_importer(sys.argv[1:])
