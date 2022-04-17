The live-calculation mini-language
==================================

Halo properties that are stored in the database can be re-processed using a set of powerful tools that are designed to pull out insight into the data. Using this framework with `Timestep.calculate_all` or `Halo.calculate_for_descendants` can result in far faster evaluation than would be achieved by manually evaluating the equivalent on each individual halo.

The language used to express these operations is python-like but _not_ actually python. The rest of this document explains the language, first through examples and then with a list of possible operations.

Examples
--------

Let's suppose you have an example timestep `ts` and halo `h`, e.g. by calling

```python
import tangos
ts = tangos.get_timestep(...)
h  = tangos.get_halo(...)
```

One can gather properties that do not explicitly exist as properties within the database, but do not require a new calculation using simulation data. For example, the virial velocity, "Vvir" is calculated using only the virial mass and virial radius already calculated. However, because it doesn't exist yet as a halo property, `h['Vvir']` will return an error. Instead, you must perform a a "live-calculation" of "Vvir". `h.calculate('Vvir()')` will return the virial velocity of the halo. Since this is similar to calling a function, there are parenthesis associated with live calculated values.

Similarly, this method can be used within the `calculate_all` and `calculate_for_descendants` functions, e.g. `ts.calculate_all("Vvir()")` will reaturn the newly calculated virial velocity for all halos in the step. The same syntax would apply to `calculate_for_descendants`

There are some live calculations that exist which take arguments. For example, `at(r,property)` returns the value of the profile property `"property"` at radius `r`. While the second argument taken by the function *must* be an already existing halo profile property, the first argument could be either a number or even a halo property itself. Here are some examples of how one might use this function with the example profile property `"dm_density_profile"`. The values for `r` are in units of kpc.

`at(5.0,dm_density_profile)` returns the dm density at 5.0 kpc

`at(Rhalf_V,dm_density_profile)` returns the dm density at the V-band half light radius

`at(Rvir/2,dm_density_profile)` returns the dm density at half of the virial radius of the halo


This syntax works the same in `h.calculate` or `ts.calculate_all` or `h.calculate_for_descendants`. Note as well that it is possible to do arithmetic on your inputs (`Rvir/2`, `Rhalf_V*2`, `Rhalf_V+10`, etc would all work). Similarly, one can input operations on multiple halo properties, for example

`at(5.0,ColdGasMass_encl/GasMass_encl)` returns the fraction of cold/total gas within the inner 5 kpc of a given halo.

Some functions, rather than return a property, return a linked object. For example, the function `later(N)` returns a given halo's descendant halo N snapshots forward in time. The purpose of this is to connect a halo's properties at a given step to those of that halo N steps in the future (or past if you use the `earlier` function). To get properties from these links, add your target property after the function following a period. All of the above live calculation syntax also applies. For example:

`ts.calculate_all('later(5).Mvir', 'Mvir')` returns the the virial mass of each halo 5 snapshots later and the current virial mass of each halo in the current step

`ts.calculate_all('earlier(10).Vvir()')` returns the virial mass of each halo 10 snapshots earlier than the current step.

`ts.calculate_all('earlier(2).at(Rvir/2,GasMass_encl')` returns the gas mass within half of the virial radius of each halo's main progenitor 2 snapshots previous.


Special case use for histogram properties
-----------------------------------------

For *histogram* properties (currently these are just `SFR_histogram` and `BH_mdot_histogram`), the live calculation language is also the interface to special use of the stored histograms.

Let's take the SFR as an example. If you have a halo `h`, and ask for `h['SFR_histogram']`, you just get a SFR histogram back as you'd expect, one bin per 20 Myr by default. However, the database is actually storing *chunks* of the star formation history and automatically recreating it for you on the *major progenitor* branch.

You can instead request the SFR summed over *all* branches by typing `h.calculate("reassemble(SFR_histogram, 'sum')")`. Similarly, for a BH accretion history you could do `h.calculate("BH.reassemble(BH_mdot_histogram, 'sum')")`._

If you want to manually handle the reassembly, one useful option is `h.calculate("reassemble(SFR_histogram, 'place')")`. This correctly zero-pads the histogram, but does not fill in any of the data from preceding steps, so you are free to do that yourself.

Under the hood, this is implemented using the `reassemble` property of `TimeChunkedProperty` which you can find in `properties.__init__.py`. In principle it's therefore possible to implement further methods for reconstructing SFR where even more complex manipulations of the little mini-history chunks is undertaken.

**Technical note**: to access the data that is actually stored in the database, as opposed to the default reconstruction, you can ask for `h.calculate("raw(SFR_histogram)")`. The default data access `h['SFR_histogram']` or `h.calculate("SFR_histogram")`_ actually expands to something equivalent to `h.calculate("reassemble(SFR_histogram")`, and the default parameter to `reassemble` is `major`, which (as previously stated) sums only over the major progenitor branch.


Getting information on linked objects
----------------------------------------

Often halos have several linked objects associated with them. For example, any given halo may have several black holes and so
 will have several black hole objects in the database linked to it.
Live calculations using the `link()` function will return an object linked to the halo under a given name having the maximum or minimum
of a given quantity among all other linked objects of the same name associated with that same halo. The general syntax is as follows.

```
h.calculate('link(my_link_name, link_property, "max/min", constraint1, constraint2, ...constrantN)')
```

An example would be the following, looking among black holes linked to their host halo via the "BH" link name.

```
h.calculate('link(BH,BH_mass,"max")')
```

The above command will return an object linked under the "BH" name to halo h.
The black hole that is returned will be the one that has the largest value of mass (based on the property called "BH_mass" of the linked black hole objects).

It is also possible to put any arbitrary number of constraints on which link you want other than simply the maximum or minimum of some value. For example:

```
h.calculate('link(BH, BH_mass, "max", BH_central_distance<10)')
```

This will return the same as the first example, but this time returning the black hole
with maximum mass among only those that are within 10 kpc of halo center. Any number of additional constraints like this can be used.
Once you've decided how you want to pick the linked object, you can return any of that object's properties.

```
h.calculate('link(BH, BH_mass, "max", BH_central_distance<10, BH_mass>1e6).BH_mdot')
```

This will return the accretion rate of the most massive linked black hole within halo 6 that has a mass of at least
1e6 solar masses and is within 10 kpc of halo center.

Finally, one can also get a link to a progenitor with a particular property. For example, if you want to find the
progenitors of a galaxy where the `SFR` property was maximum, and return its `mass` at that time, you could use:

```
h.calculate('find_progenitor(SFR, "max").mass')
```

General Syntax Notes
------------
- a given live calculation function, `f()`, returns a value using already calculated properties of a halo
- usage: `h.calculate('f()')`, `ts.calculate_all('f()')`, `h.calculate_for_descendants('f()')`
- live calculations can take in arguments, including halo properties
- `f(property)` calls the function `f`, passing the halo property `property` for each target halo. Note that no additional quotes are needed around `property`
- `f(23)`, `f(23.0)` and `f("twenty three")` call `f` with the literal integer/float/string arguments specified. Single or double quotes can be used (`'twenty three'` and `"twenty three"` are both fine, but not `'twenty three"`)
- In general, for any input that takes a numeric value one can use a single-value halo property instead of a number
- All functions can implicitly access halo properties, so that (for example) `Vvir()` returns the virtual velocity without having to specify manually that it should calculate this from `Rvir` and `Vvir`
- If a function returns a halo link (i.e. a link to another object with its own properties) `f().value` will return the `value` stored or calculated from the linked object returned by `f()`
- Basic arithmetic works as you'd expect, so you can use `+`, `-`, `*` and `/`, as well as brackets to control precedence, e.g. `f(Mgas+Mstar)` returns the value of `f` taking the sum of the properties `Mgas` and `Mstar` for each target halo as input.
- live calculation functions and link functions can be combined. For example, given a property function `F` and link function `L`, one can do `L(...).F(...)` where F will calculate a property given the properties from the link function results and its own inputs.
- live calculation functions can be nested, e.g. given `f1` and `f2`, `f1(5,f2(Mvir))` will return the value of `f1` given, as its second argument, the value of `f2` with the halo property `Mvir` as input.

List of built-in mini-language functions
----------------------------------------

Note that _string_ inputs must have quotes when used, but property names and _expressions_ do not need quotes.


**Intrinsic object information**

* `halo_number()`:returns halo number of target
* `t()`: returns simulation time of target
* `z()`: returns simulation redshift of target
* `a()`: returns simulation scalefactor of target
* `NDM()`: returns number of DM particles in halo
* `NStar()`: returns number of star particles in halo
* `Ngas()`: returns number of gas particles in halo
* `finder_id()`: returns the halo number in the original finder output (which may differ from
  `halo_number()`)

**Mathematics and logic**

* Binary operators: `*`, `/`, `+`, `-`, `**` (power)
* Binary logic operators: `<`, `>`, `&`, `|`
* Unary operators: `abs`, `sqrt`, `log`, `log10`
* Unary logic operator: `!` (not)

**Links**

*  `earlier(n)`: returns main progenitor halo n snapshots previous to current snapshot.
    Inputs:

    - *n* (integer): number of snapshots

* `later(n)`: returns descendant halo n snapshots forward in time.
    Inputs:

    - *n* (integer): number of snapshots

* `match(s)`: returns the best match for an object in the named simulation or timestep.
   Inputs:

    - *s* (string): the name of the simulation or timestep to link to

* `link(link_name, [property_name, property_criterion, [constraint1, ...]])`: Finds a named
   link where the linked object satisfies a criterion and, optionally, some constraints.
   Inputs:

    - *link_name* (expression): the name of the link to follow, e.g. `BH`.
    - *property_name* (expression): the name of the target property to base a decision on, e.g. `BH_mass`
    - *property_criterion* (string): either `'max'` or `'min'` to pick out either the link with
       maximum or minimum value of the target property
    - *constraint1*, ... (expression): an expression returning a boolean that the object must satisfy,
       e.g. `BH_mass>1e8`

* `find_progenitor(property_name, property_criterion)`: Finds the progenitor which satisfies the
   given criterion.
   Inputs:

    - *property_name* (expression): the name of the property to evaluate, e.g. `SFR`.
    - *property_criterion* (string): either `'max'` or `'min'` to pick out either the progenitor with
      the maximum or minimum value of the target property

* Redirection operator `.`: finds a property in the linked object, e.g. `find_progenitor(SFR, 'max').mass` gets `mass`
  at the time of maximum `SFR`.

**Array extraction**

* `array[i]`: array indexing, where `array` is an expression and `i` is an integer.

* `at(position, property_name)`: Get the value of the array at a given position.
   The meaning of the position is determined by the property implementer, but could be a physical radius
   for example. Inputs:

   - *position* (float or expression): the location to evaluate at
   - *property_name* (expression): the array to interpolate

* `array_smooth(property, npix)`: returns a smoothed version of an array. Inputs:

    - *property* (expression): the name of the array to operate on, e.g. `SFR_histogram`
    - *npix* (integer): the number of pixels FWHM for the Gaussian smoothing kernel

**Array reassembly**

* `raw(property)`: returns the raw value as stored in the database. Currently only used for histogram properties; see discussion of these above.
  Inputs:

  - *property* (expression)

* `reassemble(property, reassembly_type)`: controls the way the raw value is turned into a science-ready value. Currently only used for histogram properties; see discussion of these above. Inputs:

  - *property* (expression)
  - *reassembly_type*: the default choice is `'major'` which returns the
  property evaluated over the major progenitor branch. The most useful alternative is
  `'sum'` which instead sums over all progenitors.
