import numpy as np
import numpy.testing as npt
from pytest import raises as assert_raises

import tangos
import tangos.core.dictionary
import tangos.core.halo
import tangos.core.halo_data
import tangos.core.simulation
import tangos.core.timestep
import tangos.testing as testing
import tangos.testing.simulation_generator
from tangos.relation_finding import tree


def setup_module():
    testing.init_blank_db_for_testing()

    generator = tangos.testing.simulation_generator.SimulationGeneratorForTests()
    generator.add_timestep() # ts1
    generator.add_objects_to_timestep(7)

    generator.add_timestep() # ts2
    generator.add_objects_to_timestep(7)
    generator.link_last_halos()

    generator.add_timestep() # ts3
    generator.add_objects_to_timestep(6)
    generator.link_last_halos_using_mapping({1:1, 2:1, 3:2, 4:3, 5:4, 6:5, 7:6}) # ts2->ts3: merger of halos 1 & 2

    generator.add_timestep() # ts4
    generator.add_objects_to_timestep(6)
    generator.link_last_halos()

    generator.add_timestep() # ts5
    generator.add_objects_to_timestep(5)
    generator.link_last_halos_using_mapping({1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 1})  # ts4->ts5: merger of halos 1 & 6

    generator.add_timestep() # ts6
    generator.add_objects_to_timestep(5)
    generator.link_last_halos()

    # these properties are present on all halos except halo 3 of ts1, so that the handling
    # of missing values can be tested for a float, an integer and an array column.
    # NB the integer property must fit in a 32 bit integer, since HaloProperty.data_int is
    # a plain Integer column, which is only dynamically sized on sqlite.
    for ts_number in range(1, 7):
        for halo in tangos.get_timestep("sim/ts%d" % ts_number).halos:
            if ts_number == 1 and halo.halo_number == 3:
                continue
            halo["Mvir"] = 1e10 * float(halo.NDM)
            halo["int_prop"] = 1000 + halo.halo_number
            halo["profile"] = np.arange(4) * float(halo.NDM)

    # finder_id is a BigInteger, so it can hold identifiers too large to survive a round
    # trip through floating point; give ts1 such identifiers to check they are preserved
    for halo in tangos.get_timestep("sim/ts1").halos:
        halo.finder_id = np.iinfo(np.int64).max - 10 - halo.halo_number

    tangos.core.get_default_session().commit()

    # a second simulation containing black holes, to verify that trees are not specific
    # to halos
    bh_generator = tangos.testing.simulation_generator.SimulationGeneratorForTests("sim_bh")
    bh_generator.add_timestep() # ts1
    bh_generator.add_bhs_to_timestep(4)

    bh_generator.add_timestep() # ts2
    bh_generator.add_bhs_to_timestep(3)
    bh_generator.link_last_halos_using_mapping({1: 1, 2: 2, 3: 3, 4: 3},
                                              adjust_masses=False,
                                              object_typecode=1) # BHs 3 & 4 merge

    bh_generator.add_timestep() # ts3
    bh_generator.add_bhs_to_timestep(3)
    bh_generator.link_last_bhs()

    # setup the default options, so that these can change in the config without changing the tests
    tree.mergertree_min_fractional_weight = 0.02
    tree.mergertree_min_fractional_NDM = 0.0
    tree.mergertree_max_nhalos = 30
    tree.mergertree_timeout = 15.0
    tree.mergertree_max_hops = 500

def teardown_module():
    tangos.core.close_db()

def test_default_tree_has_correct_structure():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    mt.construct()
    assert mt.summarise()=="1(1(1(1(1(1),2(2))),6(6(7(7)))))"

    mt = tree.MergerTree(tangos.get_halo("sim/ts6/2"))
    mt.construct()
    assert mt.summarise() == "2(2(2(2(3(3)))))"

def test_filter_tree_by_minweight():
    old = tree.mergertree_min_fractional_weight
    try:
        tree.mergertree_min_fractional_weight = 0.8
        mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
        mt.construct()
        assert mt.summarise() == "1(1(1(1(1(1),2(2)))))"
    finally:
        tree.mergertree_min_fractional_weight = old

def test_filter_tree_by_NDM():
    old = tree.mergertree_min_fractional_NDM
    try:
        tree.mergertree_min_fractional_NDM = 0.2
        mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
        mt.construct()
        assert mt.summarise()=="1(1(1(1(1(1),2(2))),6))"
    finally:
        tree.mergertree_min_fractional_NDM = old


def test_tree_is_constructed_lazily():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    assert mt._nodes is None
    assert len(mt) == 12
    assert mt._nodes is not None

def test_canonical_order_is_major_progenitor_first():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    # depth-first, following the major progenitor branch to its end before backtracking
    assert [str(obj.path) for obj in mt.objects] == \
           ["sim/ts6/halo_1", "sim/ts5/halo_1", "sim/ts4/halo_1", "sim/ts3/halo_1",
            "sim/ts2/halo_1", "sim/ts1/halo_1", "sim/ts2/halo_2", "sim/ts1/halo_2",
            "sim/ts4/halo_6", "sim/ts3/halo_6", "sim/ts2/halo_7", "sim/ts1/halo_7"]

def test_structural_accessors():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    base = tangos.get_halo("sim/ts6/1")
    merging = tangos.get_halo("sim/ts4/6")

    assert mt.descendant(base) is None
    assert mt.descendant(merging) == tangos.get_halo("sim/ts5/1")

    assert mt.progenitors(tangos.get_halo("sim/ts5/1")) == [tangos.get_halo("sim/ts4/1"),
                                                            tangos.get_halo("sim/ts4/6")]
    assert mt.progenitors(tangos.get_halo("sim/ts1/1")) == []

    assert mt.depth(base) == 0
    assert mt.depth(merging) == 2
    assert mt.index(base) == 0
    assert mt.objects[mt.index(merging)] == merging

    assert base in mt
    assert tangos.get_halo("sim/ts6/2") not in mt
    with assert_raises(ValueError):
        mt.depth(tangos.get_halo("sim/ts6/2"))

    assert mt.weight(base) == 1.0
    # the major progenitor carries more weight than the object merging into it
    assert mt.weight(tangos.get_halo("sim/ts4/1")) > mt.weight(merging)

def test_timesteps():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    assert [ts.extension for ts in mt.timesteps] == ["ts6", "ts5", "ts4", "ts3", "ts2", "ts1"]

    # a tree that stops early does not report the timesteps it never reaches
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"), max_hops=2)
    assert [ts.extension for ts in mt.timesteps] == ["ts6", "ts5", "ts4"]

def test_walk_depth():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))

    groups = list(mt.walk_depth())

    # every object appears exactly once
    walked = [obj for group in groups for obj in group]
    assert sorted(obj.id for obj in walked) == sorted(obj.id for obj in mt.objects)
    assert len(walked) == len(mt)

    # each group is a single timestep, and timesteps run from latest to earliest
    for group in groups:
        assert all(obj.timestep.id == group[0].timestep.id for obj in group)
    times = [group[0].timestep.time_gyr for group in groups]
    assert times == sorted(times, reverse=True)

    assert [[str(obj.path) for obj in group] for group in groups] == \
           [["sim/ts6/halo_1"],
            ["sim/ts5/halo_1"],
            ["sim/ts4/halo_1", "sim/ts4/halo_6"],
            ["sim/ts3/halo_1", "sim/ts3/halo_6"],
            ["sim/ts2/halo_1", "sim/ts2/halo_2", "sim/ts2/halo_7"],
            ["sim/ts1/halo_1", "sim/ts1/halo_2", "sim/ts1/halo_7"]]

def test_walk_depth_reversed():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    forwards = [[obj.id for obj in group] for group in mt.walk_depth()]
    backwards = [[obj.id for obj in group] for group in mt.walk_depth(reverse=True)]
    assert backwards == forwards[::-1]

def test_walk_branches():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))

    branches = list(mt.walk_branches())

    assert [[str(obj.path) for obj in branch] for branch in branches] == \
           [["sim/ts6/halo_1", "sim/ts5/halo_1", "sim/ts4/halo_1", "sim/ts3/halo_1",
             "sim/ts2/halo_1", "sim/ts1/halo_1"],
            ["sim/ts3/halo_1", "sim/ts2/halo_2", "sim/ts1/halo_2"],
            ["sim/ts5/halo_1", "sim/ts4/halo_6", "sim/ts3/halo_6", "sim/ts2/halo_7",
             "sim/ts1/halo_7"]]

    # no more than one object per timestep within a branch
    for branch in branches:
        timestep_ids = [obj.timestep.id for obj in branch]
        assert len(set(timestep_ids)) == len(timestep_ids)

    # the first object of the first branch is the base object; the first object of every
    # subsequent branch has been seen already, and is the descendant of the second object
    assert branches[0][0] == mt.base_object
    seen = set()
    for i, branch in enumerate(branches):
        if i > 0:
            assert branch[0].id in seen
            assert mt.descendant(branch[1]) == branch[0]
        seen.update(obj.id for obj in branch)

    # excluding the repeated first objects, every object appears exactly once
    unrepeated = [obj for branch in branches[:1] for obj in branch] + \
                 [obj for branch in branches[1:] for obj in branch[1:]]
    assert sorted(obj.id for obj in unrepeated) == sorted(obj.id for obj in mt.objects)

    # consecutive objects within a branch are linked
    for branch in branches:
        for descendant, progenitor in zip(branch[:-1], branch[1:]):
            assert mt.descendant(progenitor) == descendant

def test_walk_branches_on_linear_tree():
    """A tree with no mergers should yield exactly one branch"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/2"))
    branches = list(mt.walk_branches())
    assert len(branches) == 1
    assert [str(obj.path) for obj in branches[0]] == \
           ["sim/ts6/halo_2", "sim/ts5/halo_2", "sim/ts4/halo_2", "sim/ts3/halo_2",
            "sim/ts2/halo_3", "sim/ts1/halo_3"]

def test_calculate_all():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    mvir, ndm = mt.calculate_all(lambda: Mvir, lambda: NDM())

    assert len(mvir) == len(mt)
    assert len(ndm) == len(mt)

    # results are aligned with tree.objects
    for i, obj in enumerate(mt.objects):
        npt.assert_allclose(mvir[i], 1e10*float(obj.NDM))
        npt.assert_allclose(ndm[i], float(obj.NDM))

    # string and lambda forms agree
    mvir_from_string, = mt.calculate_all("Mvir")
    npt.assert_allclose(mvir_from_string, mvir)

    assert mt.calculate_all() == []

def test_calculate_all_gap_in_float_column():
    """A gap in a column of floats is marked by NaN, as it is in numpy generally"""
    # this tree contains sim/ts1/3, the one halo for which no properties were stored
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/2"))
    gap = mt.index(tangos.get_halo("sim/ts1/3"))

    mvir, = mt.calculate_all(lambda: Mvir)

    # rows are retained, so that the alignment with tree.objects is preserved
    assert len(mvir) == len(mt)
    assert mvir.dtype == np.float64
    assert np.isnan(mvir[gap])
    assert not np.isnan(np.delete(mvir, gap)).any()

def test_calculate_all_gap_in_integer_column():
    """A gap in an integer column is marked by None, since promoting the column to float
    to make room for NaN would lose precision on large identifiers"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/2"))
    gap = mt.index(tangos.get_halo("sim/ts1/3"))

    int_prop, = mt.calculate_all("int_prop")

    assert len(int_prop) == len(mt)
    assert int_prop.dtype == object
    assert int_prop[gap] is None
    for i, obj in enumerate(mt.objects):
        if i != gap:
            assert int_prop[i] == 1000 + obj.halo_number

def test_calculate_all_preserves_large_integers():
    """Integer columns keep their exact values, which float promotion would destroy"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    finder_id, = mt.calculate_all("finder_id()")

    assert finder_id.dtype == np.int64
    for i, obj in enumerate(mt.objects):
        assert finder_id[i] == obj.finder_id
        # the ts1 identifiers are chosen to be beyond the reach of float64
        # (NB done in python ints, since the round trip overflows int64)
        if obj.timestep.extension == "ts1":
            assert int(finder_id[i]) != int(float(int(finder_id[i])))

def test_calculate_all_gap_in_array_column():
    """A gap in an array-valued column is marked by None, rather than by an array of NaNs
    of some invented length"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/2"))
    gap = mt.index(tangos.get_halo("sim/ts1/3"))

    profile, = mt.calculate_all("profile")

    assert len(profile) == len(mt)
    assert profile.dtype == object
    assert profile[gap] is None
    for i, obj in enumerate(mt.objects):
        if i != gap:
            npt.assert_allclose(profile[i], np.arange(4)*float(obj.NDM))

def test_calculate_all_columns_without_gaps_are_typed():
    """Without gaps, columns are converted exactly as TimeStep.calculate_all would"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1")) # contains no incomplete halo
    mvir, int_prop, profile = mt.calculate_all("Mvir", "int_prop", "profile")

    assert mvir.dtype == np.float64
    assert int_prop.dtype == np.int64
    assert profile.shape == (len(mt), 4)
    assert profile.dtype == np.float64

def test_repeated_calculate_all_does_not_poison_the_session():
    """Each query deliberately loads objects with incomplete property collections, so it
    must not be run in the tree's own session"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))

    int_prop_first, = mt.calculate_all("int_prop")
    mvir_second, = mt.calculate_all("Mvir")
    profile_third, = mt.calculate_all("profile")

    assert (int_prop_first != None).all() # noqa: E711 - object array, so `is not None` will not do
    npt.assert_allclose(mvir_second, [1e10*float(obj.NDM) for obj in mt.objects])
    npt.assert_allclose(profile_third, [np.arange(4)*float(obj.NDM) for obj in mt.objects])

    # ordinary property access is likewise unaffected
    assert tangos.get_halo("sim/ts6/1")["Mvir"] == 1e10*900

def test_calculate_all_with_no_values_at_all():
    """With nothing to go on, there is no evidence of what type the column should be"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    nonexistent, = mt.calculate_all("nonexistent_property")
    assert len(nonexistent) == len(mt)
    assert nonexistent.dtype == object
    assert all(value is None for value in nonexistent)

def test_calculate_all_gaps_match_sanitize_false_elsewhere():
    """The representation of a gap should be the same as TimeStep.calculate_all gives"""
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/2"))
    from_tree = mt.calculate_all("int_prop", "profile")

    for obj, int_prop, profile in zip(mt.objects, *from_tree):
        expected = obj.timestep.calculate_all("dbid()", "int_prop", "profile", sanitize=False)
        expected = {row[0]: row[1:] for row in expected.T}[obj.id]
        assert (int_prop is None) == (expected[0] is None)
        assert (profile is None) == (expected[1] is None)
        if int_prop is not None:
            assert int_prop == expected[0]
            npt.assert_allclose(profile, expected[1])

def test_walks_slice_properties():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    mvir, ndm = mt.calculate_all(lambda: Mvir, lambda: NDM())

    for objects, mvir_here, ndm_here in mt.walk_depth(mvir, ndm):
        assert len(mvir_here) == len(objects)
        for obj, m, n in zip(objects, mvir_here, ndm_here):
            npt.assert_allclose(m, mvir[mt.index(obj)])
            npt.assert_allclose(n, ndm[mt.index(obj)])

    for objects, mvir_here in mt.walk_branches(mvir):
        assert len(mvir_here) == len(objects)
        for obj, m in zip(objects, mvir_here):
            npt.assert_allclose(m, mvir[mt.index(obj)])

    # lists are acceptable in place of arrays
    labels = [str(obj.halo_number) for obj in mt.objects]
    for objects, labels_here in mt.walk_depth(labels):
        assert labels_here == [str(obj.halo_number) for obj in objects]

def test_tree_of_black_holes():
    """Trees are built from tangos objects in general, not specifically halos"""
    mt = tree.MergerTree(tangos.get_item("sim_bh/ts3/1.1"))
    assert [str(obj.path) for obj in mt.objects] == \
           ["sim_bh/ts3/BH_1", "sim_bh/ts2/BH_1", "sim_bh/ts1/BH_1"]

    mt = tree.MergerTree(tangos.get_item("sim_bh/ts3/1.3"))
    assert mt.summarise() == "3(3(3,4))"
    assert [[str(obj.path) for obj in branch] for branch in mt.walk_branches()] == \
           [["sim_bh/ts3/BH_3", "sim_bh/ts2/BH_3", "sim_bh/ts1/BH_3"],
            ["sim_bh/ts2/BH_3", "sim_bh/ts1/BH_4"]]

def test_timeout_truncates_tree():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    assert not mt.truncated
    assert len(mt) == 12

    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"), timeout=-1.0)
    assert mt.truncated
    assert len(mt) == 1 # only the base object survives

def test_no_timeout_by_default():
    assert tree.MergerTree(tangos.get_halo("sim/ts6/1")).timeout is None

def test_thinning_parameters_can_be_passed_explicitly():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"), min_fractional_weight=0.8)
    assert mt.summarise() == "1(1(1(1(1(1),2(2)))))"

    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"), min_fractional_NDM=0.2)
    assert mt.summarise() == "1(1(1(1(1(1),2(2))),6))"

def test_construct_is_idempotent():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    mt.construct()
    nodes = mt._nodes
    mt.construct()
    assert mt._nodes is nodes
    mt.construct(force=True)
    assert mt._nodes is not nodes
    assert len(mt) == 12

def test_str_and_summarise():
    mt = tree.MergerTree(tangos.get_halo("sim/ts6/1"))
    assert mt.summarise() == "1(1(1(1(1(1),2(2))),6(6(7(7)))))"
    # __str__ is a human-readable rendering with one line per timestep plus connectors
    assert "1" in str(mt)
    assert str(mt).count("\r\n") > 6
