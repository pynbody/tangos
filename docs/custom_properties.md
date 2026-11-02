<!-- Stage 4b (Claude, 2026-10-29): the tutorial half of this page (up to "Using the
particle data") became tutorials/custom_properties.rst. This reference half stays
here, unchanged apart from get_halo -> get_object, for stage 5 to turn into
reference/property_calculation_api. Its examples have not been run in the rebuild. -->

Property calculations: regions, preloop and links
=================================================

This page continues the [custom properties tutorial](tutorials/custom_properties.rst),
which you should read first: it covers writing a property class, making tangos find
it, and calculating properties from particle data with `tangos write`. What follows
are three further things a property class can do.


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
import tangos
import numpy as np

class ExampleHaloProperty(PropertyCalculation):
    names = "my_parent_halo"

    def calculate(self, particle_data, existing_properties):
        offsets = np.linalg.norm(existing_properties['shrink_center'] - self.centres, axis=1)
        offsets[offsets<1e-5] = np.inf # exclude self!
        inside_mask = offsets<self.radii
        if np.any(inside_mask):
            return tangos.get_object(self.dbid[offsets.argmin()])
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

Note also that, by returning a `Halo` (retrieved from the database using `get_object`), the framework automatically
understands we are creating a link to another halo rather than a numerical property.

After the `my_parent_halo` property has been written by `tangos write`, it will be available within
live calculations (for instance one could ask for `my_parent_halo.dm_density_profile` to get the density profile of the
parent halo, if `dm_density_profile` has also been written to the database).
