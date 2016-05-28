#!/usr/bin/env python2.7


from halo_db import core
from halo_db import parallel_tasks
from halo_db.tools.crosslink import TimeLinker


def run_dbwriter(argv):
    parallel_tasks.mpi_sync_db(core.get_default_session())
    writer = TimeLinker()
    writer.parse_command_line(argv)
    writer.run_calculation_loop()


if __name__ == "__main__":
    parallel_tasks.launch(run_dbwriter, 2, [sys.argv[1:]])

