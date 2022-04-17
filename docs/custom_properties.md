How to define custom properties
===============================

_Tangos_ is at its most powerful when you define custom properties that get calculated for each halo.
A simple example of a property is the mass, the position, or a mass profile.

In this tutorial, we'll build some simple properties and apply them to a database. Before you start,
it's important to have a working _tangos_ database. If you don't have that already, follow one of the
[making your first database](first_steps.md) tutorials and/or [download a working SQLite database](data_exploration.md)
and then return to this document.


Basic concept and a minimal example
-----------------------------------

In _tangos_, each property is associated with a unique name and with a python class that provides a
way to calculate the property and, possibly, provides some ancillary information about the property.

Therefore the simplest possible example consists of a single class which defines a custom method,
`calculate`, and a class static member `names`.
Create a new python file and name it `mytangosproperty.py` with the following contents:

```python
from tangos.properties import PropertyCalculation

class ExampleHaloProperty(PropertyCalculation):
    names = "myproperty"

    def calculate(self, particle_data, existing_properties):
        return 42.0
```

This corresponds to a property in the database with the name `myproperty` which is always `42.0` for every halo.
Note that the classes you create _must_ be derived from `tangos.properties.PropertyCalculation` as above,
otherwise _tangos_ will not understand your intention.

Letting tangos know about your property
---------------------------------------

Now we need to make your class visible to _tangos_.
The easiest way to do this is to set the environment variable `TANGOS_PROPERTY_MODULES`. With
bash-like shells this corresponds to `export TANGOS_PROPERTY_MODULES=mytangosproperty`. With csh-like shells,
you'd instead use `setenv TANGOS_PROPERTY_MODULES mytangosproperty`.  (If you are using `tangos` from within a
single python session, you can also just type `import mytangosproperty` in that python session.)

*Alternatively*, since version 1.0.10, you can use the [egg entry point](https://packaging.python.org/specifications/entry-points/)
`tangos.property_modules`. This requires creating a `setup.py` for your new package. Within `setup.py`, your
`setup` call will look something like this:

```
setup(name='my_tangos_properties',
      version=...,
      description=...,
      ...,
      entry_points={"tangos.property_modules" : [
          "mytangosproperty = mytangosproperty"
      ]}
      )
```

In case you prefer not to have a `setup.py` for your properties, the environment variable option will remain
for the foreseeable future.


Using your property
-------------------

Now you can start using your property right away. In fact, because this property does not require any of the
simulation data to be loaded you can use it in a live session. In the web interface, anywhere that you can type
in a property name you can write `myproperty()` and `42.0` will be returned. Or, within a python session, try
the following:

```python
halo = tangos.get_halo("tutorial_changa/%960/halo_1")
print(halo.calculate("myproperty()")) # -> 42.0
```

Note that the property is being calculated on-the-fly (known as _live calculation_).
It is not actually stored in the database. That is why
the function-like syntax (`myproperty()` rather than `myproperty`) is required.

You can calculate the property for multiple halos. For example, from the web interface use the
timestep view and add a column with heading `myproperty()`; equivalently, from python

```python
timestep = tangos.get_timestep("tutorial_changa/%960") # get the first timestep in the database
print(timestep.calculate_all("myproperty()")) # -> array of 42.0s, one for each halo
```


A more useful example that continues to work on-the-fly
-------------------------------------------------------

So far, our example isn't going to tell us much of interest. But let's now build a more interesting case where
the property corresponds to a _processed_ version of data that's already in the database. This will continue to
work on-the-fly.

Let's suppose you want to return the x position of a halo. Since in the earlier tutorial we already calculated
the center of each halo, this can be inferred from existing properties without touching the particle data.
Modify `mytangosproperty.py` to read:

```python
from tangos.properties import PropertyCalculation

class ExampleHaloProperty(PropertyCalculation):
    names = 'my_x'

    def calculate(self, particle_data, existing_properties):
        return existing_properties['shrink_center'][0]

    # NOTE: this example is incomplete - see below
```
Next, either restart your python session or manually reload your `mytangosproperty` module. If you are using the
web server front end, you will need to restart it.

Once the code is reloaded you can try calculating `my_x()` for a halo:

```python
halo = tangos.get_halo("tutorial_changa/%960/halo_1")
print(halo['shrink_center'][0])
print(halo.calculate("my_x()")) # -> same value
```

This should work successfully. However loading across multiple halos will, at this point, fail:
```python
timestep = tangos.get_timestep("tutorial_changa/%960")
print(timestep.calculate_all("my_x()"))  # -> error
```
You'll see a message something like `KeyError: "No such property 'shrink_center'"`. What happened?

Tangos needs to know a bit more to perform SQL query optimisations
-------------------------------------------------------------------

When _tangos_ calculates across multiple halos it needs to minimise queries to the
underlying database. The query optimisation means it has to know in advance that a live property
will require a certain piece of data.

Add the following code to the bottom of your class:
```python
    def requires_property(self):
        return ["shrink_center"]
```
This tells _tangos_ that your code will be accessing the `shrink_center` property, and the underlying SQL query
will retrieve this data appropriately. This is all you need - if you reload python or your module,
you should now find that the above `calculate_all` example now works correctly.

The method `requires_property` returns a list and can include multiple properties if you need them.


Calculating multiple properties in one class
--------------------------------------------

It's often desirable to calculate multiple closely-related properties in one class. This is straight-forward; you
just add the names and return a list or tuple of values:

```python
from tangos.properties import PropertyCalculation

class ExampleHaloProperty(PropertyCalculation):
    names = "my_x", "my_y", "my_z"

    def calculate(self, particle_data, existing_properties):
        my_sc = existing_properties['shrink_center']
        return my_sc[0], my_sc[1], my_sc[2]

    def requires_property(self):
        return ["shrink_center"]

```

Using the particle data
-----------------------

Let's now implement a property that requires access to the underlying particle data. In this example, we'll calculate
the velocity dispersion of the halo:

```python
from tangos.properties.pynbody import PynbodyPropertyCalculation
import numpy as np

class ExampleHaloProperty(PynbodyPropertyCalculation):
    names = "velocity_dispersion"

    def calculate(self, particle_data, existing_properties):
        return np.std(particle_data['vel'])
```

By deriving from `PynbodyPropertyCalculation` instead of `PropertyCalculation`, you are indicating to _tangos_
that this property can only be calculated with reference
to the _pynbody_-loaded original particle data associated with the halo. (Note that there is an [equivalent
for yt](using_with_yt.md), and you can make your [own customised loaders](custom_input_handlers.md)
by delving a bit more into detail.)
Anyway, if you try to calculate this from within a
standard `tangos` session you'll run into difficulties (specifically you'll see a `RuntimeError`). That's because
_tangos_ refuses to automatically load particle data; it assumes this isn't really what you'd like to happen in
a typical analysis session.

We instead need to use the `tangos write` which populates the database from the underlying particle data. Type

```
tangos write velocity_dispersion --for tutorial_changa
```

from your UNIX shell to do so. Don't forget you can also run this in
parallel, or with various optimisations --
see the [basic tutorials](first_steps.md) for more information on this. Your code does
not need to be aware of the parallelisation mode or any other details; it's always handed a complete set of particles
for the halo it's operating on.

Once the command is complete (or even while it's still running -- results will be committed every few minutes) you
should be able to find your `velocity_dispersion` properties associated with halos. It'll appear in the web interface
as a default property, and you can get it from a tangos halo object with either `halo['velocity_dispersion']` or
`halo.calculate('velocity_dispersion')`. Similarly, you can get it for a whole timestep with
`timestep.calculate_all('velocity_dispersion')`. The `pynbody` backend always provides particle
data in _physical units_ as defined by `pynbody`, i.e. kpc, km/s, Gyr etc. So, the velocity dispersion
you just calculated will be in km/s.

Using the particle data outside the halo
----------------------------------------

If you're used to working with `pynbody` you might expect `particle_data.ancestor` to give you access to the simulation
as a whole. _You should not rely on this being the case_.
Depending on the parallelisation method, your property calculation may or may not have access to the rest of the simulation.

If you'd like to perform calculations on data that is not within the halo as defined by your halo-finder, you must
define a _region_ that the framework can provide to your property. The simplest example is to use a spherical region
that includes all particles within a sphere encompassing the halo. This would, for example, bring subhalos into your
calculation if that's what you want.

Suppose we want to calculate the virial radius of a halo. That might well require going beyond the boundaries that the
halo finder defined. In the following example, we go out to a sphere of twice the radius the halo finder discovered:


```python
from tangos.properties.pynbody import PynbodyPropertyCalculation
import pynbody

class ExampleHaloProperty(PynbodyPropertyCalculation):
    names = "my_virial_radius"

    def calculate(self, particle_data, existing_properties):
        with pynbody.transformation.translate(particle_data, -existing_properties['shrink_center']):
            return pynbody.analysis.halo.virial_radius(particle_data)

    def region_specification(self, existing_properties):
        return pynbody.filt.Sphere(existing_properties['max_radius']*2,
                                   existing_properties['shrink_center'])

    def requires_property(self):
        return ["shrink_center", "max_radius"]
```

Here, the `region_specification` method indicates that we don't just want the halo finder's particles; we want a region
of our own making. The nature of a `region_specification` in general will depend on the backend in use; for the default
_pynbody_ backend, it is a filter that describes how to cut out the target region from the full simulation (in physical
units). Because we already found the radius and center of the halo in a previous step, this is straight-forward;
we just ask for a big sphere centered on the halo center. The reason for using this approach is that it now
doesn't matter what parallelisation strategy is in use when you actually invoke `tangos write my_virial_radius`
– your code sees the complete region that you asked for.

Your code does, however, need to take responsibility for putting back the data as you found it. Sometimes multiple
properties may operate on the same region in sequence. There is no way in general for the framework to guarantee the
data is untouched (other than reloading it from disk, which would be prohibitively expensive when calculating many
properties simultaneously) – you _must_ do it yourself, even if your calculation fails with an exception since _tangos_
will just print that exception and continue onto further calculations. `pynbody` offers
tools for these types of guarantee; in this case the `calculate` function uses the `with translate(...)` construction
which guarantees to put particles back where they started.

Accessing other halos and returning them as the result of a calculation
-----------------------------------------------------------------------

Suppose we want to know whether a halo is within the sphere carved out by another more massive halo
-- in other words, to identify a 'parent' halo.
(Subfind does this automatically, but some other halo finders may not.) Implementing a calculation to figure this
out requires access to the position of all halos within a timestep. This is possible by adding a `preloop` member;
consider the following implementation:

```python
from tangos.properties import PropertyCalculation
from tangos import get_halo
import numpy as np

class ExampleHaloProperty(PropertyCalculation):
    names = "my_parent_halo"

    def calculate(self, particle_data, existing_properties):
        offsets = np.linalg.norm(existing_properties['shrink_center'] - self.centres, axis=1)
        offsets[offsets<1e-5] = np.inf # exclude self!
        inside_mask = offsets<self.radii
        if np.any(inside_mask):
            return get_halo(self.dbid[offsets.argmin()])
        else:
            return None

    def preloop(self, particle_data, timestep_object):
        self.centres, self.radii, self.dbid, self.NDM = \
            timestep_object.calculate_all("shrink_center","max_radius","dbid()","NDM()")

    def requires_property(self):
        return ["shrink_center", "max_radius"]

```

The `preloop` method is called once per timestep, rather than within the 'loop' over halos.
It allows us to gather the centers and radii of all the existing halos.

Note also that, by returning a `Halo` (retrieved from the database using `get_halo`), the framework automatically
understands we are creating a link to another halo rather than a numerical property.

After the `my_parent_halo` property has been written by `tangos write`, it will be available within
live calculations (for instance one could ask for `my_parent_halo.dm_density_profile` to get the density profile of the
parent halo, if `dm_density_profile` has also been written to the database).
