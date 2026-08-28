"""Tests for the lambda front-end to the live calculation system.

Note that names used inside the test lambdas must not clash with names defined at
module level in this file, since the whole point of the module under test is that
names resolving in the surrounding python scope may be interpolated. The names
deliberately used for that purpose are prefixed with 'python_'.
"""

import numpy as np
from pytest import mark, raises as assert_raises

import tangos
import tangos.core.halo
import tangos.live_calculation as lc
import tangos.live_calculation.parser as parser
import tangos.testing as testing
import tangos.testing.simulation_generator
from tangos.live_calculation import from_lambda
from tangos.live_calculation.from_lambda import (
    ControlFlowError,
    LambdaCalculationError,
    to_calculation,
    to_calculations,
)


def setup_module():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()

    ts1 = generator.add_timestep()
    ts1_h1, ts1_h2 = generator.add_objects_to_timestep(2)
    ts1_h1['dummy_property_1'] = np.arange(0, 100.0)
    ts1_h1['dummy_property_3'] = -2.5

    ts1_h1_bh = tangos.core.halo.BH(ts1, 1)
    tangos.get_default_session().add(ts1_h1_bh)
    ts1_h1_bh['BH_mass'] = 1000.0
    ts1_h1['BH'] = ts1_h1_bh

    generator.add_timestep()
    ts2_h1, = generator.add_objects_to_timestep(1)
    ts2_h1['dummy_property_3'] = 5.0
    generator.link_last_halos()


def teardown_module():
    tangos.core.close_db()


def assert_matches_string(function, string, **kwargs):
    """Assert that the lambda generates the same calculation as the string"""
    from_lambda_result = to_calculation(function, **kwargs)
    from_string_result = parser.parse_property_name(string)
    assert str(from_lambda_result) == str(from_string_result)
    return from_lambda_result


#: (lambda, equivalent string) pairs, checked in test_matches_parser
PARITY_CASES = [
    (lambda: dummy_property_1, "dummy_property_1"),
    (lambda: Vvir(), "Vvir()"),
    (lambda: at(3.0, dummy_property_1), "at(3.0,dummy_property_1)"),
    (lambda: at(Rvir/2, dm_density_profile), "at(Rvir/2,dm_density_profile)"),
    (lambda: BH.BH_mass, "BH.BH_mass"),
    (lambda: a.b.c, "a.b.c"),
    (lambda: later(5).Mvir, "later(5).Mvir"),
    (lambda: earlier(2).at(Rvir/2, GasMass_encl), "earlier(2).at(Rvir/2,GasMass_encl)"),
    (lambda: a.b(), "a.b()"),
    (lambda: a().b, "a().b"),
    (lambda: dm_density_profile[3], "dm_density_profile[3]"),
    (lambda: a()[0], "a()[0]"),
    (lambda: later(5).a[0], "later(5).a[0]"),
    (lambda: (Mvir, Rvir), "(Mvir,Rvir)"),
    (lambda: (Mvir,), "(Mvir)"),
    (lambda: f((Mvir, Rvir)), "f((Mvir,Rvir))"),
    (lambda: reassemble(SFR_histogram, 'sum'), "reassemble(SFR_histogram,'sum')"),
    (lambda: raw(SFR_histogram), "raw(SFR_histogram)"),
    (lambda: link(BH, BH_mass, "max"), 'link(BH,BH_mass,"max")'),
    (lambda: link(BH, BH_mass, "max", BH_central_distance < 10),
     'link(BH,BH_mass,"max",BH_central_distance<10)'),
    (lambda: find_progenitor(SFR, "max").mass, 'find_progenitor(SFR,"max").mass'),
    (lambda: abs(dummy_property_2), "abs(dummy_property_2)"),
    (lambda: sqrt(Mvir), "sqrt(Mvir)"),
    # arithmetic and comparison operators
    (lambda: Mgas + Mstar, "Mgas+Mstar"),
    (lambda: Mgas - Mstar, "Mgas-Mstar"),
    (lambda: Mgas * Mstar, "Mgas*Mstar"),
    (lambda: Mgas / Mstar, "Mgas/Mstar"),
    (lambda: Mgas ** Mstar, "Mgas**Mstar"),
    (lambda: -Mvir, "-Mvir"),
    (lambda: ~has_property(Mvir), "!has_property(Mvir)"),
    (lambda: ~has_property(Mvir), "~has_property(Mvir)"),
    (lambda: Mgas > Mstar, "Mgas>Mstar"),
    (lambda: Mgas < Mstar, "Mgas<Mstar"),
    (lambda: Mgas >= Mstar, "Mgas>=Mstar"),
    (lambda: Mgas <= Mstar, "Mgas<=Mstar"),
    (lambda: Mgas == Mstar, "Mgas==Mstar"),
    (lambda: Mgas != Mstar, "Mgas!=Mstar"),
    (lambda: (Mgas > 1) & (Mstar < 2), "Mgas>1 & Mstar<2"),
    (lambda: (Mgas > 1) | (Mstar < 2), "Mgas>1 | Mstar<2"),
    # reflected operators, where the calculation is on the right hand side
    (lambda: 2 + Mvir, "2+Mvir"),
    (lambda: 2 - Mvir, "2-Mvir"),
    (lambda: 2 * Mvir, "2*Mvir"),
    (lambda: 2 / Mvir, "2/Mvir"),
    (lambda: 2 ** Mvir, "2**Mvir"),
    (lambda: 2 < Mvir, "Mvir>2"),
    (lambda: 2 >= Mvir, "Mvir<=2"),
    # literals
    (lambda: f(1), "f(1)"),
    (lambda: f(1.5), "f(1.5)"),
    (lambda: f(1e6), "f(1e6)"),
    (lambda: f("a string"), 'f("a string")'),
    (lambda: f('a string'), "f('a string')"),
    # things python evaluates for us before we ever see them
    (lambda: f(2 + 3), "f(5)"),
    (lambda: f("a" + " string"), 'f("a string")'),
    (lambda: f(f"{2}"), 'f("2")'),
    (lambda: f("%d" % 2), 'f("2")'),
]


@mark.parametrize("function, string", PARITY_CASES)
def test_matches_parser(function, string):
    assert_matches_string(function, string)


def test_returned_types():
    assert isinstance(to_calculation(lambda: Mvir), lc.StoredProperty)
    assert isinstance(to_calculation(lambda: Vvir()), lc.LiveProperty)
    assert isinstance(to_calculation(lambda: later(1).Mvir), lc.Link)
    assert isinstance(to_calculation(lambda: (Mvir, Rvir)), lc.MultiCalculation)
    assert isinstance(to_calculation(lambda: 1.5), lc.FixedNumericInput)
    assert isinstance(to_calculation(lambda: "hello"), lc.FixedInput)
    assert isinstance(to_calculation(lambda: Mvir + 1), lc.BuiltinFunction)


def test_numeric_literals_keep_their_type():
    assert isinstance(to_calculation(lambda: f(2))._inputs[0].proxy_value(), int)
    assert isinstance(to_calculation(lambda: f(2.0))._inputs[0].proxy_value(), float)
    assert to_calculation(lambda: f(1e-9))._inputs[0].proxy_value() == 1e-9
    # note the parser generates negate(1.5) for a literal -1.5, whereas python has
    # already folded the sign into the constant by the time we see it
    assert to_calculation(lambda: f(-1.5))._inputs[0].proxy_value() == -1.5
    assert to_calculation(lambda: f(True))._inputs[0].proxy_value() == 1


def test_all_parser_operators_are_available():
    """Every operator understood by the string parser must have a lambda spelling"""
    available = set(from_lambda._BINARY_OPS.values())
    available.update(from_lambda._COMPARISON_OPS.values())
    available.update(from_lambda._UNARY_OPS.values())
    for _symbol, function_name in parser.IN_OPS + parser.UNARY_OPS:
        assert function_name in available


def test_to_calculations():
    result = to_calculations(lambda: Mvir, lambda: Rvir/2)
    assert isinstance(result, lc.MultiCalculation)
    assert str(result) == str(parser.parse_property_names("Mvir", "Rvir/2"))


# ---------------------------------------------------------------- name resolution

python_radius = 3.0
python_basis = 'max'
python_calculation = parser.parse_property_name("Rvir/2")
python_sub_lambda = lambda: Mgas + Mstar
python_module = np
python_shadowing_property = 4.0
python_multiple_calculation = parser.parse_property_name("(Mgas,Mstar)")
python_difference = lambda a, b: a-b
python_nullary_lambda = lambda: rho
python_unary_lambda = lambda x: rho


def python_named_function(a, b=1.0):
    return a*b


def test_python_values_are_interpolated():
    assert_matches_string(lambda: at(python_radius, dm_density_profile),
                          "at(3.0,dm_density_profile)")
    assert_matches_string(lambda: at(python_radius/3, dm_density_profile),
                          "at(1.0,dm_density_profile)")
    assert_matches_string(lambda: link(BH, BH_mass, python_basis),
                          'link(BH,BH_mass,"max")')


def test_calculations_are_inlined():
    assert_matches_string(lambda: at(python_calculation, dm_density_profile),
                          "at(Rvir/2,dm_density_profile)")


def test_sub_lambdas_are_inlined():
    assert_matches_string(lambda: at(python_sub_lambda, dm_density_profile),
                          "at(Mgas+Mstar,dm_density_profile)")


def test_sub_lambdas_can_be_called_or_used_as_values():
    """A python lambda standing for a calculation works with or without the ()"""
    assert_matches_string(lambda: python_nullary_lambda/mvir, "rho/mvir")
    assert_matches_string(lambda: python_nullary_lambda()/mvir, "rho/mvir")
    assert_matches_string(lambda: python_nullary_lambda()/mvir, "rho/mvir",
                          python_names='always')


def test_calling_a_sub_lambda_with_arguments_rejected():
    assert_rejected(lambda: python_nullary_lambda(2), contains="takes no arguments")


def test_python_functions_are_called_with_the_calculations_as_arguments():
    """A python function of several arguments builds part of the calculation"""
    assert_matches_string(lambda: python_difference(MDM, Mgas), "MDM-Mgas")
    assert_matches_string(lambda: python_difference(MDM, Mgas), "MDM-Mgas",
                          python_names='always')
    assert_matches_string(lambda: python_difference(MDM, 2)/Mgas, "(MDM-2)/Mgas")
    assert_matches_string(lambda: at(python_difference(Rvir, 1), dm_density_profile),
                          "at(Rvir-1,dm_density_profile)")


def test_python_functions_may_be_named_functions_with_defaults():
    assert_matches_string(lambda: python_named_function(Mgas), "Mgas*1.0")
    assert_matches_string(lambda: python_named_function(Mgas, 2), "Mgas*2")
    # unlike a live calculation function, a python function may take keyword
    # arguments, since it is genuinely called
    assert_matches_string(lambda: python_named_function(Mgas, b=2), "Mgas*2")


def test_calling_a_python_function_with_the_wrong_arguments_rejected():
    """The python function is really called, so its arguments must match"""
    message = assert_rejected(lambda: python_unary_lambda()/mvir,
                              contains="python_unary_lambda")
    assert "argument" in message
    assert_rejected(lambda: python_difference(Mgas), contains="argument")
    assert_rejected(lambda: python_difference(MDM, Mgas, Mvir), contains="argument")


def test_python_functions_are_tangos_names_in_never_mode():
    assert_matches_string(lambda: python_unary_lambda()/mvir,
                          "python_unary_lambda()/mvir", python_names='never')
    assert_matches_string(lambda: python_difference(MDM, Mgas),
                          "python_difference(MDM,Mgas)", python_names='never')


def test_python_function_used_as_a_value_rejected():
    assert_rejected(lambda: at(python_difference, dm_density_profile),
                    contains="python_difference")


def test_if_usable_mode_ignores_uninterpolatable_values():
    """In 'if_usable' mode a name bound to something that isn't a calculation is a property"""
    assert_matches_string(lambda: python_module.pi, "python_module.pi")


def test_always_mode_uses_the_python_value():
    assert_matches_string(lambda: python_module.pi * Mvir,
                          "%r*Mvir" % np.pi, python_names='always')
    assert_matches_string(lambda: python_shadowing_property,
                          "4.0", python_names='always')


def test_never_mode_ignores_the_python_scope():
    assert_matches_string(lambda: python_radius, "python_radius",
                          python_names='never')
    assert_matches_string(lambda: at(python_radius, dm_density_profile),
                          "at(python_radius,dm_density_profile)",
                          python_names='never')


def test_python_builtins_do_not_shadow_live_calculation_functions():
    """abs and sum exist in python, but here they must be live calculation functions"""
    assert_matches_string(lambda: abs(dummy_property_2), "abs(dummy_property_2)")
    assert_matches_string(lambda: sum(dummy_property_2), "sum(dummy_property_2)")


def test_closure_variables_are_interpolated():
    def enclosing_scope():
        enclosed_radius = 5.0
        return to_calculation(lambda: at(enclosed_radius, dm_density_profile))

    assert str(enclosing_scope()) == str(parser.parse_property_name(
        "at(5.0,dm_density_profile)"))


def test_closure_variables_respect_never_mode():
    def enclosing_scope():
        dm_density_profile = "not actually a profile"
        return to_calculation(lambda: at(5.0, dm_density_profile),
                              python_names='never')

    assert str(enclosing_scope()) == str(parser.parse_property_name(
        "at(5.0,dm_density_profile)"))


def test_unknown_python_names_mode():
    with assert_raises(ValueError):
        to_calculation(lambda: Mvir, python_names='something_else')


# ----------------------------------------------------------------------- rejections

def assert_rejected(function, expected_error=LambdaCalculationError, contains=None):
    with assert_raises(expected_error) as excinfo:
        to_calculation(function)
    if contains is not None:
        assert contains in str(excinfo.value)
    return str(excinfo.value)


def test_conditional_rejected():
    assert_rejected(lambda: Mgas if Mstar else Mvir, ControlFlowError, contains="if/else")


def test_boolean_operators_rejected():
    assert_rejected(lambda: Mgas and Mstar, ControlFlowError, contains="'and'")
    assert_rejected(lambda: Mgas or Mstar, ControlFlowError, contains="'or'")
    assert_rejected(lambda: not Mgas, ControlFlowError, contains="'~x'")


def test_chained_comparison_rejected():
    assert_rejected(lambda: 1 < Mgas < 2, ControlFlowError)


def test_membership_and_identity_rejected():
    assert_rejected(lambda: Mgas in Mstar, ControlFlowError, contains="'in' test")
    assert_rejected(lambda: Mgas is None, ControlFlowError, contains="'is' test")


def test_comprehensions_and_generators_rejected():
    assert_rejected(lambda: [x for x in Mgas], ControlFlowError)
    assert_rejected(lambda: tuple(x for x in Mgas), ControlFlowError)
    assert_rejected(lambda: {x: 1 for x in Mgas}, ControlFlowError)


def test_control_flow_inside_a_nested_lambda_rejected():
    assert_rejected(lambda: f(lambda: Mgas if Mstar else Mvir), ControlFlowError)


def test_argument_unpacking_rejected():
    assert_rejected(lambda: f(*Mgas), ControlFlowError, contains="unpacking")


def test_control_flow_in_a_called_function_rejected():
    """The static check cannot see inside an ordinary function, so there is a backstop"""
    def branching_function(value):
        return 1 if value else 2

    with assert_raises(ControlFlowError):
        to_calculation(lambda: branching_function(Mgas))
    with assert_raises(ControlFlowError):
        to_calculation(lambda: branching_function(Mgas), python_names='always')


def test_lambda_with_arguments_rejected():
    message = assert_rejected(lambda halo: halo.Mvir, contains="no arguments")
    assert "lambda halo" in message


def test_non_function_rejected():
    assert_rejected("Mvir")
    assert_rejected(42)


def test_keyword_arguments_rejected():
    assert_rejected(lambda: at(radius=3.0), contains="keyword arguments")


def test_list_rejected():
    assert_rejected(lambda: [Mgas, Mstar], contains="tuple")


def test_empty_tuple_rejected():
    assert_rejected(lambda: (), contains="empty tuple")


def test_none_rejected():
    assert_rejected(lambda: None, contains="None")


def test_unsupported_operators_rejected():
    assert_rejected(lambda: Mgas // 2, contains="'//'")
    assert_rejected(lambda: Mgas % 2, contains="'%'")
    assert_rejected(lambda: Mgas ^ Mstar, contains="'^'")
    assert_rejected(lambda: 2 // Mgas, contains="'//'")


def test_slicing_rejected():
    assert_rejected(lambda: dm_density_profile[1:3], contains="slice")


def test_non_numeric_index_rejected():
    assert_rejected(lambda: dm_density_profile["3"], contains="must be a number")


def test_calling_an_expression_rejected():
    assert_rejected(lambda: (Mgas + Mstar)(), contains="cannot call")


def test_linking_from_an_expression_rejected():
    assert_rejected(lambda: python_multiple_calculation.Mvir,
                    contains="cannot follow a link")


def test_f_string_of_calculation_rejected():
    assert_rejected(lambda: f(f"{Mgas}"), ControlFlowError, contains="f-string")


def test_self_reference_rejected():
    recursive = lambda: recursive + 1
    assert_rejected(recursive, contains="refers to itself")


def test_error_message_includes_source():
    message = assert_rejected(lambda: Mgas if Mstar else Mvir, ControlFlowError)
    assert "Mgas if Mstar else Mvir" in message


# --------------------------------------------------------------------- evaluation

EVALUATION_CASES = [
    (lambda: dummy_property_3, "dummy_property_3"),
    (lambda: dummy_property_3 * 2, "dummy_property_3*2"),
    (lambda: 2 * dummy_property_3, "2*dummy_property_3"),
    (lambda: abs(dummy_property_3), "abs(dummy_property_3)"),
    (lambda: -dummy_property_3, "-dummy_property_3"),
    (lambda: dummy_property_1[3], "dummy_property_1[3]"),
    (lambda: dummy_property_3 < 0, "dummy_property_3<0"),
    (lambda: BH.BH_mass, "BH.BH_mass"),
    (lambda: later(1).dummy_property_3, "later(1).dummy_property_3"),
]


@mark.parametrize("function, string", EVALUATION_CASES)
def test_evaluation_matches_string_version(function, string):
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate(to_calculation(function)),
                       halo.calculate(string))


def test_evaluation_of_multiple_calculations():
    halo = tangos.get_halo("sim/ts1/1")
    calculation = to_calculation(lambda: (dummy_property_3, dummy_property_1[2]))
    assert np.allclose(calculation.values_sanitized([halo]), [[-2.5], [2.0]])


def test_calculate_all_from_lambda():
    timestep = tangos.get_timestep("sim/ts1")
    from_lambda_result, = timestep.calculate_all(
        to_calculation(lambda: dummy_property_3 * 2))
    from_string_result, = timestep.calculate_all("dummy_property_3*2")
    assert np.allclose(from_lambda_result, from_string_result)
    assert np.allclose(from_lambda_result, [-5.0])


# ------------------------------- lambdas passed directly, wherever strings are accepted

def test_calculate_accepts_a_lambda():
    halo = tangos.get_halo("sim/ts1/1")
    assert np.allclose(halo.calculate(lambda: dummy_property_3*2),
                       halo.calculate("dummy_property_3*2"))


def test_calculate_all_accepts_lambdas_alongside_strings():
    timestep = tangos.get_timestep("sim/ts1")
    from_lambda_result, from_string_result = timestep.calculate_all(
        lambda: dummy_property_3*2, "dummy_property_3*2")
    assert np.allclose(from_lambda_result, from_string_result)
    assert np.allclose(from_lambda_result, [-5.0])


def test_calculate_for_descendants_accepts_a_lambda():
    halo = tangos.get_halo("sim/ts1/1")
    from_lambda_result, = halo.calculate_for_descendants(lambda: dummy_property_3)
    from_string_result, = halo.calculate_for_descendants("dummy_property_3")
    assert np.allclose(from_lambda_result, from_string_result)
    assert np.allclose(from_lambda_result, [-2.5, 5.0])


def test_calculate_for_progenitors_accepts_a_lambda():
    halo = tangos.get_halo("sim/ts2/1")
    from_lambda_result, = halo.calculate_for_progenitors(lambda: dummy_property_3)
    assert np.allclose(from_lambda_result, [5.0, -2.5])


def test_calculations_can_be_built_from_lambdas():
    """Anywhere a Calculation can be assembled from strings, a lambda also works"""
    assert str(lc.MultiCalculation(lambda: Mvir, "Rvir")) \
           == str(parser.parse_property_name("(Mvir,Rvir)"))
    assert str(lc.Link(lambda: BH, "BH_mass")) \
           == str(parser.parse_property_name("BH.BH_mass"))


def test_unusable_calculation_type_rejected():
    halo = tangos.get_halo("sim/ts1/1")
    with assert_raises(TypeError):
        halo.calculate(42)
