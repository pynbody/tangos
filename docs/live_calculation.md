Live calculation language reference
===================================

Tangos stores properties of objects (halos, black holes, groups, ...) in a database. Often
what you actually want is not a stored property but something derived from one: the virial
velocity rather than the mass and radius; the density at half the virial radius rather than
the whole density profile; the mass of a halo's descendant five snapshots later.

You could of course loop over objects in python and work these things out one at a time. But
that means a database query per object, and for a timestep with thousands of halos it is
slow. The _live calculation_ system instead lets you describe the derived quantity once, and
then evaluates it for every object of interest in a small number of queries. Using
`TimeStep.calculate_all`, `Halo.calculate_for_progenitors` or `Halo.calculate_for_descendants`
with a live calculation is typically far faster than the equivalent python loop.

This document explains how to write live calculations, starting from simple examples and
building up to links, histogram reassembly and the full list of built-in functions.

If you have not yet got a database to play with, see the [data exploration
tutorial](data_exploration.md).


Two ways to write a calculation
-------------------------------

A live calculation can be written either as a **python lambda taking no arguments**, or as a
**string** in a small mini-language. The two are exactly equivalent — they produce the same
calculation, and run at the same speed:

```python
h.calculate(lambda: at(Rvir/2, dm_density_profile))
h.calculate("at(Rvir/2, dm_density_profile)")
```

For most interactive and scripted work the lambda form is the more comfortable of the two.
It is genuine python, so your editor highlights it, matches your brackets, and complains if
you leave one unclosed; and there is no quoting to get right.

The lambda form is new in tangos 1.12.0. The string form has always been available, and
remains fully supported; it is the one to reach for whenever a calculation has to exist as
text: typed into the [web interface](data_exploration_webserver.md), read from a
configuration file or a command line argument, or stored in a database. It is also the form
used in most existing tangos scripts and in the tutorial notebooks.

Both forms may be passed anywhere a calculation is expected — `Halo.calculate`,
`TimeStep.calculate_all`, `Halo.calculate_for_progenitors`,
`Halo.calculate_for_descendants` — and the two may be mixed freely in a single call:

```python
ts.calculate_all(lambda: later(5).Mvir, "Mvir")
```

The examples below give both forms side by side. Everything else in this document applies to
both, except for the two sections at the end that describe the wrinkles specific to each.


First steps
-----------

Suppose you have a timestep `ts` and a halo `h`:

```python
import tangos
ts = tangos.get_timestep(...)
h  = tangos.get_halo(...)
```

The property names used in the examples below (`Mvir`, `dm_density_profile`, `SFR_histogram`
and so on) are illustrative; substitute whatever your own database contains, which you can
check with `h.keys()`.

The simplest possible calculation is a single stored property. This is no more useful than
`h['Mvir']`, but it establishes the pattern:

```python
h.calculate(lambda: Mvir)
h.calculate("Mvir")
```

Arithmetic on stored properties works as you would expect, using `+`, `-`, `*`, `/`, `**`
and brackets:

```python
h.calculate(lambda: Mvir/Rvir)
h.calculate("Mvir/Rvir")
```

The same calculation can be applied to every object in a timestep at once. This is where the
system earns its keep:

```python
ratio, = ts.calculate_all(lambda: Mvir/Rvir)
```

or along the major progenitor branch of a single halo:

```python
mass_history, time = h.calculate_for_progenitors(lambda: Mvir, lambda: t())
```

Note that `calculate_all`, `calculate_for_progenitors` and `calculate_for_descendants` take
any number of calculations and return one array for each. Only objects for which _all_ of
the requested calculations succeed are returned, so the arrays always line up with each other.

### Live properties

Some quantities are not stored in the database at all, but can be computed on demand from
things that are. These are called _live properties_, and they are written like function
calls. For example `t()`, `z()` and `a()` return the time, redshift and scalefactor of the
snapshot the object belongs to:

```python
ts.calculate_all(lambda: Mvir, lambda: t())
ts.calculate_all("Mvir", "t()")
```

The set of live properties available depends on which property modules you have installed:
they are defined by `LivePropertyCalculation` classes, and you can write your own (see
[writing your own properties](custom_properties.md)). A common example is a virial velocity
calculated from the already-stored `Mvir` and `Rvir`:

```python
h.calculate(lambda: Vvir())
h.calculate("Vvir()")
```

The brackets are what distinguishes a live property from a stored one, so `h['Vvir']` would
fail where `h.calculate(lambda: Vvir())` succeeds. A live property works out for itself which
stored properties it needs; you never have to tell it.

Live properties can take arguments, which may be numbers, strings, stored properties, or
whole expressions. They can also be nested inside each other. All of the following are
legitimate:

```python
h.calculate(lambda: at(5.0, dm_density_profile))
h.calculate(lambda: at(Rhalf_V, dm_density_profile))
h.calculate(lambda: at(Rvir/2, dm_density_profile))
h.calculate(lambda: at(5.0, ColdGasMass_encl/GasMass_encl))
```


Arrays and profiles
-------------------

Many stored properties are arrays — density profiles, mass profiles, images, histograms. The
live calculation system can pick values out of them.

A specific element is selected by indexing with an integer, which may be negative to count
from the end:

```python
ts.calculate_all(lambda: star_mass_profile[-1])
ts.calculate_all("star_mass_profile[-1]")
```

More usefully, `at(position, array)` interpolates the array at a given position. What
"position" means is decided by whoever wrote the property — for a profile it is normally a
physical radius in kpc:

* `at(5.0, dm_density_profile)` is the dark matter density at 5 kpc;
* `at(Rhalf_V, dm_density_profile)` is the density at the V-band half light radius;
* `at(Rvir/2, dm_density_profile)` is the density at half the virial radius.

The first argument may be a number or any expression, including a stored property; the second
must be an array-valued property (possibly with arithmetic applied to it), because `at` needs
the property's own description of what its x-axis means.

`array_smooth(array, npix)` returns a Gaussian-smoothed copy of an array, and
`max`, `min`, `posmax` and `posmin` return the maximum and minimum value of an array and the
positions at which they occur:

```python
h.calculate(lambda: posmax(dm_density_profile))
h.calculate("posmax(dm_density_profile)")
```


Links and redirection
---------------------

Objects in a tangos database are linked to each other: a halo is linked to its progenitors
and descendants, to the black holes it hosts, to its counterpart in another simulation, and
to anything else a property module has chosen to record.

Some functions return a linked object rather than a value. To get a property of that object,
follow it with a `.`:

```python
ts.calculate_all(lambda: later(5).Mvir, lambda: Mvir)
ts.calculate_all("later(5).Mvir", "Mvir")
```

This returns the virial mass of each halo's descendant five snapshots later, alongside its
present virial mass. `earlier(n)` does the same for the main progenitor `n` snapshots back,
`latest()` and `earliest()` jump to the ends of the branch, and `match(name)` finds the
counterpart of the object in a named simulation or timestep. Anything that can be calculated
on an object can be calculated after a redirection, including further redirections:

```python
ts.calculate_all(lambda: earlier(10).Vvir())
ts.calculate_all(lambda: earlier(2).at(Rvir/2, GasMass_encl))
ts.calculate_all(lambda: match('tutorial_changa_blackholes').star_mass_profile[-1])
```

Links that a property module has written into the database are followed the same way, simply
by naming them:

```python
h.calculate(lambda: BH_central.BH_mass)
h.calculate("BH_central.BH_mass")
```

### Choosing between several linked objects

Often an object has several links under the same name — a halo may host several black holes,
all linked to it as `BH`. The `link()` function picks one of them out, choosing the linked
object with the maximum or minimum of some property:

```python
h.calculate(lambda: link(BH, BH_mass, "max"))
h.calculate('link(BH, BH_mass, "max")')
```

This returns the black hole linked to `h` under the name `BH` that has the largest `BH_mass`.

Any number of further constraints may be added. Each is an expression, evaluated on the
candidate objects, that must be true:

```python
h.calculate(lambda: link(BH, BH_mass, "max", BH_central_distance<10))
h.calculate('link(BH, BH_mass, "max", BH_central_distance<10)')
```

This is the most massive black hole among only those within 10 kpc of the halo centre. Having
picked the object you want, you can then ask for any of its properties:

```python
h.calculate(lambda: link(BH, BH_mass, "max", BH_central_distance<10, BH_mass>1e6).BH_mdot)
h.calculate('link(BH, BH_mass, "max", BH_central_distance<10, BH_mass>1e6).BH_mdot')
```

which gives the accretion rate of the most massive black hole that is both within 10 kpc of
the centre and above 10<sup>6</sup> solar masses.

Note that in the lambda form, comparisons that are combined with `&` or `|` need brackets
around them, because of python's operator precedence:

```python
h.calculate(lambda: link(BH, BH_mass, "max", (BH_mass>1e6) & (BH_central_distance<10)))
```

### Searching along the merger tree

`find_progenitor(property, "max"|"min")` searches the whole main progenitor branch for the
step at which a property is largest or smallest, and returns the object at that step;
`find_descendant` does the same going forwards. So to find the mass of a galaxy at the time
its star formation rate peaked:

```python
h.calculate(lambda: find_progenitor(SFR, "max").mass)
h.calculate('find_progenitor(SFR, "max").mass')
```

### Reducing over many linked objects

Where `match(name)` picks a single counterpart, `match_reduce(name, calculation, reduction)`
performs a calculation on _every_ linked object in the target simulation or timestep and then
combines the results with `'sum'`, `'mean'`, `'min'` or `'max'`. For example, the total
stellar mass of all counterparts of each halo in another simulation:

```python
ts.calculate_all(lambda: match_reduce('tutorial_changa', Mstar, 'sum'))
ts.calculate_all("match_reduce('tutorial_changa', Mstar, 'sum')")
```


Comparisons and filters
-----------------------

Comparison operators (`>`, `<`, `>=`, `<=`, `==`, `!=`) and logical operators (`&`, `|` and
logical not) return boolean arrays, which is what makes constraints inside `link()` work.
They are equally useful for filtering the output of `calculate_all`, since a calculation that
is `False` for an object is still returned — it is up to you to use it as a mask:

```python
mass, radius, is_big = ts.calculate_all(lambda: Mvir, lambda: Rvir, lambda: Mvir>1e12)
mass = mass[is_big]
```

Two functions test whether data exists at all: `has_property(name)` is true for objects that
have the named property stored, and `has_link(name)` is true for objects that have the named
link. These are most useful negated:

```python
ts.calculate_all(lambda: ~has_property(Mvir))
ts.calculate_all("!has_property(Mvir)")
```

Note the spelling difference: the string form accepts either `!` or `~` for logical not,
whereas the lambda form must use `~`, since python's `not` cannot be used (see
[below](#what-cannot-be-written-as-a-lambda)). The `~` spelling in a string is itself new in
tangos 1.12.0; older versions accept only `!`.


Histogram properties
--------------------

For _histogram_ properties (currently `SFR_histogram` and `BH_mdot_histogram`), the live
calculation system is also the interface to the way the histogram is put back together.

Take the star formation rate as an example. If you have a halo `h` and ask for
`h['SFR_histogram']`, you get an SFR history back as you would expect, one bin per 20 Myr by
default. However, what the database actually stores is a series of *chunks* of the star
formation history, one per timestep, which are automatically reassembled for you along the
*major progenitor* branch.

You can instead ask for the SFR summed over *all* branches:

```python
h.calculate(lambda: reassemble(SFR_histogram, 'sum'))
h.calculate("reassemble(SFR_histogram, 'sum')")
```

and similarly for a black hole accretion history, following the link to the black hole first:

```python
h.calculate(lambda: BH.reassemble(BH_mdot_histogram, 'sum'))
h.calculate("BH.reassemble(BH_mdot_histogram, 'sum')")
```

If you want to handle the reassembly yourself, `'place'` correctly zero-pads the histogram
onto the full time axis but does not fill in any data from preceding steps, leaving you free
to do that as you wish:

```python
h.calculate(lambda: reassemble(SFR_histogram, 'place'))
```

Under the hood this is implemented by the `reassemble` method of `TimeChunkedProperty`, which
you can find in `tangos/properties/__init__.py`. It is therefore possible to implement further
reassembly methods where more complex manipulations of the stored chunks are undertaken; see
[understanding time-histogram properties](histogram_properties.md).

**Technical note**: to get at the data exactly as stored in the database, with no
reassembly at all, ask for `raw(SFR_histogram)`. The default data access
`h['SFR_histogram']`, or equivalently `h.calculate(lambda: SFR_histogram)`, expands to
something equivalent to `reassemble(SFR_histogram)`, whose default reassembly type is
`'major'` — which, as above, sums only over the major progenitor branch.


Writing calculations as lambdas
-------------------------------

A live calculation lambda must take no arguments, and its body is a single expression:

```python
h.calculate(lambda: Vvir())
h.calculate(lambda: at(Rvir/2, dm_density_profile))
h.calculate_for_progenitors(lambda: SFR_histogram[0])
ts.calculate_all(lambda: later(5).Mvir, lambda: Mvir)
```

### How names are resolved

A name inside the lambda that does not correspond to any python variable is a database
property or a live calculation function, exactly as in the string form. This is the usual
case: `Mvir`, `dm_density_profile`, `at` and `later` are not python variables, so they are
interpreted as tangos names.

A name that _does_ refer to a python variable holding a number, a string, or another
calculation is substituted into the calculation. This is how you interpolate a value you have
worked out in python:

```python
radius = 5.0
h.calculate(lambda: at(radius, dm_density_profile))     # at(5.0,dm_density_profile)
```

A python variable holding a function is used as that function. A lambda taking no arguments
stands for a calculation in its own right, and can be written either bare or called, which
makes it easy to build up a library of reusable pieces:

```python
half_radius = lambda: Rvir/2

h.calculate(lambda: at(half_radius, dm_density_profile))    # at(Rvir/2,dm_density_profile)
h.calculate(lambda: at(half_radius(), dm_density_profile))  # the same
```

A python function taking arguments is called, with the calculations you wrote as its
arguments, so that it can assemble part of the calculation for you:

```python
def fractional_growth(now, before):
    return (now - before)/now

ts.calculate_all(lambda: fractional_growth(mass, earlier(2).mass))
# equivalent to "(mass-earlier(2).mass)/mass"
```

Note that names appearing _inside_ such a function are ordinary python names, not tangos
names — so a helper like this must take the properties it works on as arguments, rather than
naming them itself.

Python's own builtins are deliberately excluded, so that live calculation functions such as
`abs`, `max` and `min` are not shadowed by the python functions of the same name.

The one thing to watch for is a python variable that happens to share its name with a
database property: if it holds a number, string or calculation it will be interpolated, and
the property of that name will not be consulted. Avoid, for example, storing a radius in a
variable called `Rvir` and then writing `lambda: at(Rvir/2, dm_density_profile)`.

Finer control over all of this is available through the `python_names` argument of
`tangos.live_calculation.from_lambda.to_calculation`, which can force every name to be
treated as a tangos name (`python_names='never'`), or conversely make python scoping rules
apply throughout (`python_names='always'`); see its docstring for details.

### What cannot be written as a lambda

The live calculation language has no control flow, and so none of python's control flow
constructs can be used inside a lambda: `if`/`else`, `and`, `or`, `not`, `in`, `is`,
comprehensions and generator expressions. Rather than silently mis-handling them, tangos
rejects them with an explanation:

```python
>>> h.calculate(lambda: Mgas if Mstar else Mvir)
ControlFlowError: the live calculation language cannot express a conditional or boolean
short-circuit (if/else, 'and', 'or'); use '&', '|' and '~' instead
```

For element-wise logic use `&`, `|` and `~`, which do exist in the language, in place of
`and`, `or` and `not`. Similarly, use a tuple rather than a list to request several
calculations at once, and note that a property whose name happens to be a python keyword
(`class`, `lambda`, ...) has to be written using the string form.


Writing calculations as strings
-------------------------------

The string form is the original way of writing live calculations, and remains the only option
where a calculation must be typed or stored as text — most obviously in the web interface.
The syntax is deliberately python-like, but it is _not_ python, and there are a couple of
places where that matters.

### Quoting

Property names and expressions are written bare, without quotes: `at(5.0,dm_density_profile)`,
not `at(5.0,"dm_density_profile")`. Only genuine string arguments are quoted, using either
single or double quotes (`'max'` and `"max"` are both fine, but not `'max"`). Because the
calculation as a whole is a python string, it is usually easiest to write the outer quotes as
one kind and the inner ones as the other:

```python
h.calculate('link(BH, BH_mass, "max")')
h.calculate("reassemble(SFR_histogram, 'sum')")
```

### Operator precedence and associativity

This is the one part of the string form that is likely to surprise you. The mini-language does
_not_ use python's precedence rules. Instead, its operators bind in the following order,
tightest first:

    **   *   /   +   -   >   <   |   &   ==   !=   >=   <=

and then, binding _less_ tightly than any of those, the unary operators `!`, `~` (logical not)
and `-` (negation). Furthermore, every operator is **right**-associative. The consequences are
worth spelling out, because they are not what a python programmer expects:

| string             | means                | in python would mean |
|--------------------|----------------------|----------------------|
| `"a-b-c"`          | `a-(b-c)`            | `(a-b)-c`            |
| `"a/b*c"`          | `a/(b*c)`            | `(a/b)*c`            |
| `"a-b+c"`          | `a-(b+c)`            | `(a-b)+c`            |
| `"-a+b"`           | `-(a+b)`             | `(-a)+b`             |
| `"a>1 & b<2"`      | `(a>1) & (b<2)`      | `a > (1&b) < 2`      |

This behaviour is long-standing and is retained so that calculations written years ago, and
stored in databases and scripts, continue to mean what they have always meant. The practical
advice is simply to bracket anything you are not certain about: brackets mean exactly what
they do in python, and a fully bracketed expression reads identically in both forms.

The last line of the table is the one place where the unusual precedence is convenient rather
than surprising, since `"a>1 & b<2"` needs no brackets. In the lambda form ordinary python
precedence applies throughout, so there the brackets are required — but nothing else about
lambdas needs any special thought, which is the main reason to prefer them.


General syntax notes
--------------------

These apply to both forms:

* a live calculation function `f()` returns a value computed from already-stored properties
  of an object;
* functions can take arguments, including stored properties, expressions and other function
  calls, e.g. `f1(5, f2(Mvir))`;
* anywhere a number is expected, a single-valued property or an expression can be used
  instead;
* if a function returns a link to another object, `f().value` returns `value` from that
  object, and link functions and property functions can be chained arbitrarily, e.g.
  `L(...).F(...)`;
* several calculations can be requested at once by passing several arguments to
  `calculate_all`, `calculate_for_progenitors` or `calculate_for_descendants`; they may also
  be grouped into a single calculation, written `(Mvir, Rvir)` in either form, which is also
  how a group of calculations is passed to a function that expects one, e.g.
  `f((Mvir, Rvir))`;
* `Halo.calculate(..., return_description=True)` additionally returns the property class
  describing the result, which is what tells you the units and the meaning of an array's
  x-axis;
* a calculation can also be built once and reused, either with
  `tangos.live_calculation.parser.parse_property_name` (from a string) or
  `tangos.live_calculation.from_lambda.to_calculation` (from a lambda); the resulting
  `Calculation` object is accepted wherever a string or lambda is.


List of built-in functions
--------------------------

The functions below are available in any tangos installation. Individual property modules add
many more — anything defined as a `LivePropertyCalculation` becomes available as a function
here, so if you are looking for something that is not in this list, check which property
modules you have loaded and see [writing your own properties](custom_properties.md).

In the string form, _string_ inputs must be quoted, while property names and expressions must
not be. In the lambda form, ordinary python quoting applies.

**Intrinsic object information**

* `halo_number()`: the halo number of the target object
* `finder_id()`: the object's number in the original finder output, which may differ from
  `halo_number()`
* `finder_offset()`: the object's offset within the original finder output
* `dbid()`: the object's unique database id
* `t()`: the simulation time, in Gyr
* `z()`: the simulation redshift
* `a()`: the simulation scalefactor
* `NDM()`: the number of dark matter particles
* `NStar()`: the number of star particles
* `NGas()`: the number of gas particles
* `type()`, `typetag()`: the object type, as a numerical code and as a string (e.g. `halo`,
  `BH`, `group`)
* `path()`, `step_path()`: the full path of the object, and of the timestep containing it

**Mathematics and logic**

* Arithmetic operators: `*`, `/`, `+`, `-`, `**` (power)
* Comparison operators: `<`, `>`, `<=`, `>=`, `==`, `!=`
* Logical operators: `&` (and), `|` (or)
* Unary operators: `-` (negation), and logical not, written `~` in a lambda and either `!` or
  `~` in a string
* Functions: `abs(x)`, `sqrt(x)`, `log(x)` (natural logarithm), `log10(x)`

**Testing for data**

* `has_property(name)`: true where the named property is stored for the object.
   Inputs:

   - *name* (expression): the property to test for, e.g. `Mvir`

* `has_link(name)`: true where the named link exists for the object.
   Inputs:

   - *name* (expression): the link to test for, e.g. `BH`

**Links**

*  `earlier(n)`: returns the main progenitor halo n snapshots previous to the current snapshot.
    Inputs:

    - *n* (integer): number of snapshots

* `later(n)`: returns the descendant halo n snapshots forward in time.
    Inputs:

    - *n* (integer): number of snapshots

* `earliest()`, `latest()`: returns the earliest progenitor, or the latest descendant,
   available in the database.

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

* `find_progenitor(property_name, property_criterion)`, and correspondingly
   `find_descendant`: Finds the progenitor (or descendant) which satisfies the given criterion.
   Inputs:

    - *property_name* (expression): the name of the property to evaluate, e.g. `SFR`.
    - *property_criterion* (string): either `'max'` or `'min'` to pick out either the progenitor with
      the maximum or minimum value of the target property

* `match_reduce(s, calculation, reduction)`: finds all linked objects in a given target simulation
  or timestep, performs a calculation on each of them, then reduces the result in a specified way.
  Inputs:

   - *s* (string): the name of the simulation or timestep to link to
   - *calculation* (expression): the calculation to perform on each matching object
   - *reduction* (string): either `'min'`, `'max'`, `'mean'` or `'sum'`. Specifies how to
     reduce multiple results to a single per input object.

* Redirection operator `.`: finds a property in the linked object, e.g.
  `find_progenitor(SFR, 'max').mass` gets `mass` at the time of maximum `SFR`.

**Array extraction**

* `array[i]`: array indexing, where `array` is an expression and `i` is an integer, which may
  be negative to count from the end of the array.

* `at(position, property_name)`: Get the value of the array at a given position.
   The meaning of the position is determined by the property implementer, but could be a physical radius
   for example. Inputs:

   - *position* (float or expression): the location to evaluate at
   - *property_name* (expression): the array to interpolate

* `array_smooth(property, npix)`: returns a smoothed version of an array. Inputs:

    - *property* (expression): the name of the array to operate on, e.g. `SFR_histogram`
    - *npix* (integer): the number of pixels FWHM for the Gaussian smoothing kernel

* `max(property)`, `min(property)`: the maximum or minimum value of an array.

* `posmax(property)`, `posmin(property)`: the position (e.g. radius) at which an array
  reaches its maximum or minimum.

**Array reassembly**

* `raw(property)`: returns the raw value as stored in the database. Currently only used for histogram properties; see discussion of these above.
  Inputs:

  - *property* (expression)

* `reassemble(property, reassembly_type)`: controls the way the raw value is turned into a science-ready value. Currently only used for histogram properties; see discussion of these above. Inputs:

  - *property* (expression)
  - *reassembly_type*: the default choice is `'major'` which returns the
  property evaluated over the major progenitor branch. The most useful alternative is
  `'sum'` which instead sums over all progenitors.
