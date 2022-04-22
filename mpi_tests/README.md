About the MPI tests
===================

The scripts in this folder are ancillary tests. The main test suite for _tangos_ is in the `tests` folder and is run by typing the command `pytests`. That main test suite *includes* tests of the parallelisation.

The MPI tests in this folder are to be run as individual python scripts (not with pytests) and serve as a separate test of the different _backends_ (see `tangos/parallel_tasks/backends/`). The backends are interchangeable and use different methods for communicating across processes. The tests are necessarily hardwired to use the `multiprocessing` backend which only _emulates_ MPI — it is not what is used by typical production runs (which will use an actual MPI implementation).

So, that motivates having separate  MPI tests: for example, `test_mpi.py multiprocessing` will use the same backend as used by the main pytest. But one can repeat the test with proper MPI via `mpirun -np 3 test_mpi.py mpi4py` and again `mpirun -np 3 test_mpi.py pypar`. All backends should have the same behaviour and terminate saying `OK`.
