The live-calculation mini-language
==================================

Halo properties that are stored in the database can be re-processed using a set of powerful tools that are designed to pull out insight into the data. Using this framework with `Timestep.gather_property` or `Halo.property_cascade` can result in far faster evaluation than would be achieved by manually evaluating the equivalent on each individual halo. 

Examples
--------

The following examples mainly use fictional functions and properties.

- `function(property)` calls `function` with the value of the specified halo property `property` and returns the result
- `function(23)`, `function(23.0)` and `function("twenty three")` call `function` with the literal integer/float/string arguments specified
- `function()` can be used to call a function that takes no arguments. Note that `function` on its own does not work, as it would refer to a stored value
- All functions can implicitly access halo properties, so that (for example) `Vvir()` returns the virtual velocity without having to specify manually that it should calculate this from `Rvir` and `Vvir`
- `link.value` returns the `value` stored in the linked halo where the link is named `link`
- Functions and links can be chained and nested, so for example `average(biggest_BH().BH_mdot_hist)` is valid

