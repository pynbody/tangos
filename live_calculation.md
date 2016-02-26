The live-calculation mini-language
==================================

Halo properties that are stored in the database can be re-processed using a set of powerful tools that are designed to pull out insight into the data. Using this framework with `Timestep.gather_property` or `Halo.property_cascade` can result in far faster evaluation than would be achieved by manually evaluating the equivalent on each individual halo.

Examples
--------

Let's suppose you have an example timestep `ts` and halo `h`, e.g. by calling

```python
ts = db.get_timestep(...)
h  = db.get_halo(...)
```

Now:

- `ts.gather_property("Mvir")` returns the virial mass of every halo
- `ts.gather_property("Mvir","Mgas")` returns the virial mass and gas mass of every halo
- `ts.gather_property("Vvir()")` calls the live-calculation function `Vvir` to work out the virial velocity of every halo (from stored properties `Mvir` and `Rvir`)
- `ts.gather_property("at(2.0,'dm_density_profile')")` returns the DM density profile at 2 kpc
- `ts.gather_property("at(rhalf, 'dm_density_profile')")` returns the DM density profile at `rhalf`
- `ts.gather_property("BH.BH_mdot")` finds the first BH referenced by a halo and returns that BH's accretion `BH_mdot` (i.e. accretion rate) property. Note that the thing that happens to be referenced first in this way may or may not be the BH you care about so...
- `ts.gather_property("bh().BH_mdot")` picks out the most massive BH referenced by the halo and returns its accretion rate
- `ts.gather_property('bh("BH_mdot","max").BH_mass')` picks out the most rapidly accreting BH referenced by the halo and returns its mass
- `ts.gather_property('bh().BH_mass', 'bh().BH_mdot')` returns the mass and accretion rate of the most massive BH
- `ts.gather_property('bh().(BH_mass, BH_mdot)')` does *precisely* the same thing as the previous example, but more efficiently as it now only has to search *once* for the "right" BH in each halo

The exact same syntax can be used with `get_halo`.

Syntax examples
---------------

The following examples mainly use fictional functions and properties to illustrate everything that's available.

- `function(property)` calls `function` with the value of the specified halo property `property` and returns the result
- `function(23)`, `function(23.0)` and `function("twenty three")` call `function` with the literal integer/float/string arguments specified
- `function()` can be used to call a function that takes no arguments. Note that `function` on its own does not work, as it would refer to a stored value
- All functions can implicitly access halo properties, so that (for example) `Vvir()` returns the virtual velocity without having to specify manually that it should calculate this from `Rvir` and `Vvir`
- `link.value` returns the `value` stored in the linked halo where the link is named `link`
- Functions and links can be chained and nested, so for example `average(biggest_BH().BH_mdot_hist)` is valid
