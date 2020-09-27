Understanding time-histogram properties
---------------------------------------

While the primary method for getting time series of data is through the
`calculate_for_progenitors` and `calculate_for_descendants` methods provided by
`Halo` objects (see [data exploration tutorial](data_exploration.md)) the time resolution of
such methods is limited by the number of timesteps you have accessible.

Some information needs to be sampled on shorter timescales for good physical insight. 
An example is the star formation rate of galaxies which can vary on very short timescales. For
this purpose, _tangos_ offers histogram properties which are explored from the science
perspective in the [data exploration tutorials](data_exploration.md). This document aims to
explain a bit more about how they work by examining the star formation rate class provided
in the example properties.

You can find the class in `tangos.properties.pynbody.SF` or view it online 
[here](https://github.com/pynbody/tangos/blob/master/tangos/properties/pynbody/SF.py). 

Examining a  time-histogram class
---------------------------------

The first thing you'll notice is that `StarFormHistogram` is derived 
from `tangos.properties.TimeChunkedProperty`. That class provides the technical underpinnings
for _reassembly_ which we'll look at shortly. First, however, look at the `calculate` method
of the `StarFormHistogram` class:

```python
def calculate(self, halo, _):
    tmax_Gyr = 20.0 # calculate up to 20 Gyr
    nbins = int(20.0/self.pixel_delta_t_Gyr)
    M,_ = np.histogram(halo.st['tform'].in_units("Gyr"),weights=weights.in_units("Msol"),bins=nbins,range=(0,tmax_Gyr))
    t_now = halo.properties['time'].in_units("Gyr")
    M/=self.pixel_delta_t_Gyr
    M = M[self.store_slice(t_now)]

    return M
```

This method is calculating a star formation histogram for all the stars in the halo data
it has been provided. This is stored into `M` on the first line. However, it then returns
only a slice of that information (provided by the `TimeChunkedProperty.store_slice` method).

That slice represents the most recent star formation history of the object, and only that
recent activity is stored in the database. 

You can see this for yourself; starting from the [sample database](data_exploration.md), try
the following:

```python
import pylab as p
import tangos
halo = tangos.get_halo("tutorial_changa/%960/halo_1")
p.plot(halo.calculate("raw(SFR_histogram)/1e9"))
```

Note the use of the `raw()` live property which requests that the raw contents of the database,
without any further processing, are returned to us. We've also divided the result by 10^9 for
reasons that will become clear in a moment.

You will see that only a chunk of the history is returned. To make that even clearer, try
plotting the default reconstruction of the star formation rate:

```python
p.plot(halo['SFR_histogram'])
```

What is happening to reassemble the full history?
--------------------------------------------------

Under the hood, when a property is accessed without the `raw()` invocation above, the 
property description is offered the opportunity to _reassemble_ the property. 
The `TimeChunkedProperty` class takes responsibility for that, working its way back through
the merger tree and assembling the full history from the individual chunks.

It is possible to control its approach to this reassembly through another live property,
`reassemble()`. Currently three modes of reassembly are supported:

 - `major` (default): follows the major progenitor branch
 - `sum`: sums over all progenitors
 - `place`: places the raw data from this timestep in a correctly padded array for comparison
   with the two modes above.
   
Try plotting the three modes over each other:

```python
p.clf()
p.plot(halo.calculate('reassemble(SFR_histogram, "major")'),"r")
p.plot(halo.calculate('reassemble(SFR_histogram, "sum")'),"b")
p.plot(halo.calculate('reassemble(SFR_histogram, "place")'),"k:")
```

The differences between the default `major` reassembly and `sum` reassembly are discussed in the
data exploration tutorial. The `place` reassembly closely tracks the `sum` reassembly back to the
start point of the stored region, and then drops to zero.

Changing the time resolution
----------------------------

By default each histogram bin is 20 Myr long. You can change this on a per-simulation basis by
setting a simulation property. For example, for a 10 Myr time resolution for all time histograms use:

```python
sim = tangos.get_simulation("my_sim")
sim["histogram_delta_t_Gyr"] = 0.01
```

Note that it is a _very_ bad idea to change this after you have already written some time histograms
for a simulation. You will almost certainly end up getting inconsistent results. Always set `histogram_delta_t_Gyr`
before your first `tangos write`.

Reassembling live calculations
------------------------------

It is possible to define a [live calculation](live_calculation.md) that manipulates an already stored histogram
property (like `SFR_histogram`) and returns a new histogram. Because such a calculation only requires data already stored
in the database, it can be performed on the fly and reassembled accordingly in the same way as the original stored
histogram data.

The property `SpecSFR_histogram`, for example, calculates the specific star formation rate history of a halo given `SFR_histogram` and
normalizing it by the mass at every intermediate step. We can call this in the same way as any other histogram property, but
this time we are doing a live calculation, which is called like any other live calculation (in this case there are
no relevant arguments to give, but any argument taken by the live calculation will work like normal).

```
halo.calculate('reassemble(SpecSFR_histogram())')
```

It is still important to make sure that the time resolution is set correctly. For any live calculation acting on a histogram
property, the time resolution, defined for each simulation by `sim["histogram_delta_t_Gyr"]`, should be set to what it 
was when the stored histogram data was originally calculated.

Exercising caution
-------------------

Since the merger tree and the raw histogram data are stored with 
different time resolutions, there is a 
caveat: specifically,
star formation (or other histogramed activity) in the `major` and `sum` branches cannot be
distinguished between the penultimate and final timesteps. For that last time interval,
these two queries will always give the same result. This is an unavoidable technical
limitation which normally does not impact on science analysis but should be borne in mind. The
remedy is to use finer time stepping such that the problem time interval is as short as possible.