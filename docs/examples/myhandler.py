"""A minimal tangos input handler, and a property that uses it.

This is the example on the how-to page docs/how-to/custom_handlers.rst, which
includes it piece by piece; tests/test_docs_examples.py runs the commands that
page shows against it. Edit the three together.
"""

import tangos.input_handlers
from tangos import properties


class MyDataObject:
    def __init__(self, data):
        self.internal_data = data

    def __repr__(self):
        return str(self.internal_data)


class MyHandler(tangos.input_handlers.HandlerBase):

    def enumerate_timestep_extensions(self, parallel=False):
        return ["my_timestep_1", "my_timestep_2"]

    def get_timestep_properties(self, ts_extension):
        # The time orders the timesteps, and tangos write needs the redshift.
        # A real handler would read both from the snapshot.
        time_gyr, redshift = {"my_timestep_1": (1.0, 5.7),
                              "my_timestep_2": (2.0, 3.2)}[ts_extension]
        return {'time_gyr': time_gyr, 'redshift': redshift}

    def enumerate_objects(self, ts_extension, object_typetag='halo', min_halo_particles=None):
        if object_typetag != 'halo':
            return  # this handler only knows about halos

        if ts_extension == "my_timestep_1":
            num_halos = 100
        elif ts_extension == "my_timestep_2":
            num_halos = 150
        else:
            raise ValueError(f"Unknown timestep {ts_extension!r}")

        for i in range(num_halos):
            finder_offset = i  # where the object sits in the halo finder's catalogue
            finder_id = i + 1  # the halo finder's own label for it
            n_dm, n_star, n_gas = 10000 - 10 * i, 0, 0
            yield finder_offset, finder_id, n_dm, n_star, n_gas

    def load_timestep_without_caching(self, ts_extension, mode=None):
        # A real handler would load the snapshot here, and could return any
        # object at all; this one returns a description of what it would load.
        assert mode is None  # no specialised load modes are implemented
        return MyDataObject("Data for " + self._extension_to_filename(ts_extension))

    def load_object(self, ts_extension, finder_id, finder_offset, object_typetag='halo', mode=None):
        assert mode is None
        assert object_typetag == 'halo'
        timestep_data = self.load_timestep(ts_extension, mode)
        return MyDataObject(f"{timestep_data!r} halo {finder_id}")

    def match_objects(self, ts_extension1, ts_extension2, halo_min, halo_max,
                      dm_only=False, threshold=0.005, object_typetag='halo',
                      output_handler_for_ts2=None):
        assert object_typetag == 'halo'

        # Map each finder_id in the first timestep to a list of
        # (finder_id in the second timestep, weight) pairs. Here every halo
        # becomes the halo with the same finder_id, with weight 1.
        num_halos_in_both = 100
        if halo_max is not None:
            num_halos_in_both = min(num_halos_in_both, halo_max)
        return {finder_id: [(finder_id, 1.0)]
                for finder_id in range(max(halo_min, 1), num_halos_in_both + 1)}


class MyProperty(properties.PropertyCalculation):
    names = "my_fantastic_property"
    works_with_handler = MyHandler
    requires_particle_data = True

    def calculate(self, data, existing_properties):
        # tangos only ever passes this property data from MyHandler:
        assert isinstance(data, MyDataObject)

        # ... a meaningful calculation would go here ...
        return 42.0
