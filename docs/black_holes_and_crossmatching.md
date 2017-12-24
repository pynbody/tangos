Tangos Tutorial – Black holes
=============================

This tutorial covers adding black holes and crossmatching simulations to make halo-to-halo comparisons. 
It extends the [first steps with Changa+AHF tutorial](first_steps_changa+ahf.md)
by adding a simulation with black holes. The simulation is started from identical initial
conditions and therefore serves as a comparison to quantify the effect of AGN feedback.

Download, add and process the new simulation
--------------------------------------------

First, download the
[raw simulation data](http://star.ucl.ac.uk/~app/tangos/tutorial_changa_blackholes.tar.gz) required 
for this tutorial.
Unpack the tar file either in your home folder or the folder that you pointed the `TANGOS_SIMULATION_FOLDER` 
environment
variable to.

Next, refer back to the [first steps with Changa+AHF tutorial](first_steps_changa+ahf.md).
Follow all the steps there but replacing `tutorial_changa` with `tutorial_changa_blackholes`. 


Crosslink the simulations
-------------------------

Next we'll identify which halo corresponds to which across the simulations. This allows us to make one-to-one
comparisons to isolate the effects of AGN. From your UNIX shell type:

```
tangos_crosslink tutorial_changa tutorial_changa_blackholes
```

If you want to speed up this process, it can be [MPI parallelised](mpi.md).

Add black hole information
--------------------------

Black holes are added to the _tangos_ database with a specialised script that scans each simulation output
for black hole particles. Type:

```
tangos_add_bh tutorial_changa_blackholes
```


