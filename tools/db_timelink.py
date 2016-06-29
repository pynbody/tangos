#!/usr/bin/env python2.7


from tangos import core
from tangos import parallel_tasks
from tangos.tools.crosslink import TimeLinker
import sys


def run_dbwriter(argv):
    parallel_tasks.mpi_sync_db(core.get_default_session())
    writer = TimeLinker()
    writer.parse_command_line(argv)
    writer.run_calculation_loop()


if __name__ == "__main__":
    parallel_tasks.launch(run_dbwriter, 2, [sys.argv[1:]])

