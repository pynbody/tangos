Tracking in tangos
==================

Often it's helpful to track sets of particles or other tracers within a simulation.
For example, one might select inflowing or outflowing gas and then inspect its origin
at earlier timesteps or destination at later timesteps.

_Tangos_ enables this kind of tracking. In the following introduction we'll make use of the
[ChaNGA tutorial dataset](first_steps_changa+ahf.md). The steps can easily be adapted for other
formats.

Selecting particles
-------------------

The first step is to select particles of interest. Let's find the particles in a halo with temperature
less than 2e4 Kelvin. Within a python session type the following:
 
```python
import tangos, pynbody
particles = tangos.get_halo("tutorial_changa/%960/1").load()
lowT_gas = particles.gas[pynbody.filt.LowPass('temp',2e4)]
```

Now we'll use those particles to generate a  tracker:
```
from tangos import tracking
tracking.new("tutorial_changa", lowT_gas)
```

If logging is enabled you may see messages about creation of the required
database objects. The `tracking.new` function returns the identifying number of the
tracker which is unique within the simulation. Assuming this is the first tracker
you created, the number will be 1, in which case you will find you can now get the corresponding
objects throughout time:
```
particles_step_832 = tangos.get_item("tutorial_changa/%832/tracker_1").load() 
```
In the example above, tangos returns the corresponding particles at step 832. However,
the normal use for trackers is in conjunction with `tangos_writer`: we can now use the
tracked particles pretty much like any halo, as follows.

Calculating properties for trackers
-----------------------------------

Let's calculate the mean gas temperature for the tracked particles across time. For this
we leave the python session and return to the UNIX shell. Run:

```
tangos_writer mean_temp --sims tutorial_changa --type tracker
```
Here,
 * `tangos_writer` is the standard command used in the earlier [tutorial](first_steps_changa+ahf.md)
 * `mean_temp` is a simple built-in property that calculates the mean temperature of the tracked region. 
   Of course, the real power is in being able to define your [own properties](custom_properties.md), but this will do for now. 
   You do not need to make any modifications to properties to make them work with trackers; _tangos_ takes
   responsibility for figuring out and passing in the tracked particles to your calculation.
 * `--type tracker` tells _tangos_ to calculate for trackers rather than regular halos

Check the results
-----------------

Finally, let's check the results. This can be done from the web interface (where trackers appear next
to the main halo list) or in python as follows:

```python
import tangos, pylab
time, temp = tangos.get_halo("tutorial_changa/%960/tracker_1").calculate_for_progenitors("t()","mean_temp")
pylab.plot(time,temp)
```

