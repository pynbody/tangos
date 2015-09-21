Troubleshooting
-------------------------------------------
This document is a collection of common issues one might have when running the halo database.

MPI issues
-------------------------------------------
*1) Don't have an MPI compiler?*

The readme file has you to `pip install pypar` but if you don't have an MPI compiler handy this will throw an error like
```
pip install pypar
Collecting pypar
  Could not find a version that satisfies the requirement pypar (from versions: )
No matching distribution found for pypar
```
So, try this instead:
```
pip install git+ssh://git@github.com/daleroberts/pypar.git
```

*2) pypar having trouble accessing mpi routines*

For example, `OSError: libmpi.so: cannot open shared object file: No such file or directory`
This is caused by a miscomunication between what your local mpi environment is and what pypar thinks it should look for. This can be rectified by explicitly loading an mpi module. This islikely a bit different depending on the machine you are on. For example, on Pleiades:
```
module load mpi-intel/5.0.3.048
```
You will need to insert this into any submission scripts you write. If you still have issues, try re-installing pypar with the module loaded.
