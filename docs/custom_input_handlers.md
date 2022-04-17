Loading data with systems other than pynbody
============================================

The core of _tangos_ is agnostic about the way in which raw simulation data is loaded and processed.
The developers of _tangos_ use _pynbody_ for this purpose but it can be adapted for other libraries
with relative ease.

As a demonstration, embryonic _yt_ support is included; more information is in the
[using with yt](using_with_yt.md) document.

Tangos refers to systems that can load the raw simulation data as _handlers_.
The remainder of this page explores the requirements for such handlers, and how they can be exposed
for use.

Handler classes
---------------

Handlers are implemented by subclasses of `tangos.input_handlers.HandlerBase`.
To write your own, start by creating such a subclass. At a minimum will need to provide a way for _tangos_:

 - to enumerate the available simulation timesteps. This is the method
   `enumerate_timestep_extensions`.
   It returns a list of strings that name the individual timesteps. These are known as _timestep
   extensions_ because usually the filesystem path is composed of the simulation path plus the
   timestep extension.

 - to enumerate the objects within each timestep. This is the method
   `enumerate_objects`.

 - to load the data corresponding to each timestep. This is the method
   `load_timestep_without_caching`. (Alternatively one can override `load_timestep`,
    but then one should also implement a caching scheme that prevents multiple reads
    of the same data over and again.)

 - to load the data corresponding to each object. This is the method
   `load_object`.

Ideally, one also should have a method to link objects between steps, known as `match_objects`.

 So, a minimal handler, that manually exposes a simulation with two timesteps and
 100 halos per timestep, is as follows:


```python
import tangos.input_handlers

class MyDataObject(object):
    def __init__(self, data):
        self.internal_data = data

    def __repr__(self):
        return str(self.internal_data)

class MyHandler(tangos.input_handlers.HandlerBase):
    def enumerate_timestep_extensions(self):
        return ["my_timestep_1", "my_timestep_2"]

    def get_timestep_properties(self, ts_extension):
        # You can store the redshift and time with each timestep.
        # At a minimum, returning the time helps tangos order the steps

        my_time = float(ts_extension[-1]) # obviously you'd normally read this from a file...

        return {'time_gyr': my_time}

    def enumerate_objects(self, timestep_extension, object_typetag, min_halo_particles):
        if object_typetag!="halo":
            # we only know about objects called 'halo'
            return

        if timestep_extension=="my_timestep_1":
            # suppose timestep 1 has 100 halos
            for i in range(100):
                yield i
        elif timestep_extension=="my_timestep_2":
           # suppose timestep 2 has 150 halos
            for i in range(150):
                yield i
        else:
            raise ValueError("Unknown timestep %r"%timestep_extension)

    def load_timestep_without_caching(self, ts_extension, mode):
        # load mode strings can be passed in by the user to indicate specialised loading
        # requirements. Here we don't implement any such specialised approaches.
        assert mode is None

        # At this point one would construct and return the data object corresponding to
        # ts_extension. The return type can be absolutely any pynbody object. Here we just
        # make it an explanatory string.
        return MyDataObject("Data for " + self._extension_to_filename(ts_extension))

    def load_object(self, timestep_extension, object_number, object_typetag, mode):

        assert mode is None # see load_timestep_without_caching
        assert object_typetag=="halo" # see enumerate_objects

        # As with load_timestep_without_caching, the return type can be any pynbody object.
        # Here again we just return an explanatory string.
        return MyDataObject("%r halo %d"%
                           (self.load_timestep(timestep_extension, mode), object_number))


    def match_objects(self, timestep_extension1, timestep_extension2,
                      halo_min, halo_max, dm_only, threshold, object_typetag):
        assert object_typetag=="halo" # see enumerate_objects

        results = []

        # for each object in timestep 1:
        for i in range(halo_min,100):
            if i<halo_max or halo_max is None:
                results.append([(i, 1.0)]) # link it with 1.0 certainty to halo i in timestep_extension2

        return results

```

Save this file as `myhandler.py`.

Your handler class in action
----------------------------

Let's see your handler class in action. First, create a new simulation. From the UNIX shell type:

```
tangos add test_my_handler --handler=myhandler.MyHandler
```

Ensure that `myhandler.py` is in your python search path. You should see that the two timesteps are created,
with 100 and 150 halos respectively. Also from the shell, run:

```
tangos link --for test_my_handler
```

Note that once the simulation has been created you don't need to remind _tangos_ of the handler. It stores
a record of your handler with the simulation. So this timelinking automatically calls your `match_objects` function.

Next, you can see the results in python. For example

```python
import tangos
print(tangos.get_halo("test_my_handler/my_timestep_2/halo_10").earlier.load())
```

You should see the following string: `Data for $TANGOS_SIMULATION_FOLDER/test_my_handler/my_timestep_1 halo 10` (with
`$TANGOS_SIMULATION_FOLDER` appropriately expanded). Let's see what's happened here:

 - The `tangos add` command created a record of the `test_my_handler` simulation. It called your
   `enumerate_timestep_extensions` implementation which revealed the existence of two timesteps. It then called
   the `enumerate_objects` implementation which revealed the existence of halos in these timesteps. The
   relevant database objects were created.

 - From within python, you fetched the simulation `test_my_handler`, the timestep `my_timestep_2`, and finally
   the halo 10 within that timestep. You asked for the `earlier` halo, which used the links `tangos link` set up
   based on the information returned from your `match_objects` routine.

 - Then you asked to `load()` the underlying data. _Tangos_ redirected this
   request to your `load_object` routine which returned a string, which is what you see printed out. Of course in a
   realistic implementation the returned object would actually contain some particle data!


Adding properties that use your handler class
---------------------------------------------

Finally, let's add a halo property using your custom loader. (More information on adding halo properties
can be found in the [custom properties](custom_properties.md) tutorial.)

Typically, calculating halo properties requires a knowledge of the data-loading
library in use. For that reason, halo properties can be tied to specific loaders. For simplicity
you can add a new property to the bottom of your `myloader.py` file (though it doesn't have to
be in the same file, of course) as follows:

```python
from tangos import properties

class MyProperty(properties.PropertyCalculation):
    works_with_handler = MyHandler

    names = "my_fantastic_property"

    def calculate(self, data, halo):

        # Reassure yourself tangos has passed the data type you expected:
        assert isinstance(data, MyDataObject)

        # ... do a meaningful calculation here...
        result = 42.0

        return result
```

The only thing that is different from the [custom properties](custom_properties.md) tutorial is
the declaration that this property `works_with_handler`, associating it specifically with your
data handler. You can therefore be assured that `calculate` will be passed a `MyDataObject` to work
with (and the `assert` is just there to demonstrate that fact).

This new property should be available to the `tangos write`, so from your UNIX shell type:

```
tangos write my_fantastic_property --for test_my_handler
```

and finally, go back to python and verify everything worked:
```python
import tangos
print(tangos.get_halo("test_my_handler/my_timestep_1/halo_5")['my_fantastic_property'] # -> 42.0
```
