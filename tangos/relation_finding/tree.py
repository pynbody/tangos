"""Merger trees: the graph of progenitors of a tangos object.

The main entry point is the :class:`MergerTree` class, which builds the tree and offers
ways to walk over it. :class:`MergerTreeLayout` turns a tree into a two-dimensional
layout suitable for plotting; it underpins :meth:`MergerTree.plot` and the web interface.
"""

import contextlib
import math
import time

import numpy as np
from sqlalchemy.orm import object_session

from .. import core, live_calculation, temporary_halolist
from ..config import (
    mergertree_max_hops,
    mergertree_max_nhalos,
    mergertree_min_fractional_NDM,
    mergertree_min_fractional_weight,
)
from ..log import logger
from . import MultiHopAllProgenitorsStrategy


class TreeNode:
    """A single position within a :class:`MergerTree`.

    Tree nodes are an implementation detail of the tree: the user-facing methods of
    :class:`MergerTree` yield the underlying tangos objects rather than these nodes, and
    the structure of the tree is queried through the tree itself (e.g.
    :meth:`MergerTree.progenitors`). Nodes are, however, the natural place for code that
    processes the tree recursively, such as :class:`MergerTreeLayout`.
    """

    __slots__ = ('children', 'depth', 'index', 'obj', 'parent', 'weight')

    def __init__(self, obj, parent, depth, weight):
        self.obj = obj
        self.parent = parent
        self.children = []
        self.depth = depth
        self.weight = weight
        self.index = None

    def __repr__(self):
        return "<TreeNode %r at depth %d>" % (self.obj, self.depth)


class MergerTree:
    """The tree of progenitors of a given tangos object.

    The nodes of the tree are ordinary tangos objects (instances of
    :class:`tangos.core.halo.SimulationObjectBase`), which may be halos, black holes,
    groups, or any other object type; a tree simply follows whatever links are present in
    the database. Consequently the documentation below refers throughout to *objects*
    rather than halos.

    A tree is built by walking backwards in time from a base object, so every object in
    the tree is a progenitor of the base object (which is itself the root of the tree).
    Because links are followed only backwards in time, the timestep of an object is always
    strictly earlier than that of its descendant.

    Typical use is to evaluate some properties over the whole tree and then walk over it:

    >>> tree = tangos.relation_finding.MergerTree(tangos.get_halo(...))
    >>> Mvir, Rvir = tree.calculate_all(lambda: Mvir, lambda: Rvir)
    >>> for objects, Mvir_now, Rvir_now in tree.walk_depth(Mvir, Rvir):
    ...     print(len(objects), "objects at redshift", objects[0].timestep.redshift)
    ...     print("masses:", Mvir_now)
    ...     print("radii:", Rvir_now)

    The arrays returned by :meth:`calculate_all` are aligned with :attr:`objects`, and the
    walk methods slice them consistently with the objects they yield, so any number of
    them (including none) may be passed to a walk. Two walk orders are available:

    * :meth:`walk_depth` yields one group per timestep, so that all objects in a group
      share a timestep and every object in the tree is yielded exactly once;
    * :meth:`walk_branches` yields one group per branch, so that no two objects in a group
      share a timestep, and (for every branch but the first) the first object of a group
      repeats an object yielded earlier, marking where the new branch diverges.

    Trees are constructed lazily, on first use; :meth:`construct` may be called explicitly
    if you wish to control when the (potentially slow) database queries happen.

    **A note of caution:** the tree is *thinned* during construction, discarding minor
    progenitors according to the criteria described in :meth:`__init__`. Navigating with
    the ordinary tangos accessors, such as ``obj.previous`` or ``obj.calculate("earlier(1)")``,
    ignores that thinning and can therefore silently take you to an object which is not in
    the tree. Use :meth:`progenitors` and :meth:`descendant` to stay within it.
    """

    def __init__(self, base_object, min_fractional_weight=None, min_fractional_NDM=None,
                 max_nobjects=None, max_hops=None, timeout=None, must_include=None):
        """Set up a merger tree rooted at the specified object.

        The tree is not built until it is first used, or :meth:`construct` is called.

        The thinning parameters below default to the values in :mod:`tangos.config`, with
        the exception of ``timeout``, which defaults to no timeout at all. (The web
        interface passes an explicit timeout, since it must render a page promptly; for
        interactive or scripted use it is preferable for a tree to take a long time than
        for it to change shape depending on how busy the machine is.)

        :param base_object: the object to build the tree from; all other objects in the
          tree will be progenitors of it
        :type base_object: tangos.core.halo.SimulationObjectBase

        :param min_fractional_weight: discard links whose weight is below this fraction of
          the strongest link from the same object. Defaults to
          ``config.mergertree_min_fractional_weight``.

        :param min_fractional_NDM: discard objects with fewer than this fraction of the
          dark matter particles of the largest object at the same depth. Objects with no
          dark matter at all (``NDM==0``, e.g. black holes) are exempt. Defaults to
          ``config.mergertree_min_fractional_NDM``.

        :param max_nobjects: keep at most this many objects at each depth, discarding those
          with the fewest dark matter particles. Defaults to
          ``config.mergertree_max_nhalos``.

        :param max_hops: the maximum number of links to follow back from the base object.
          Defaults to ``config.mergertree_max_hops``.

        :param timeout: abandon construction after this many seconds, returning a truncated
          tree and setting :attr:`truncated`. Defaults to None, meaning no timeout.

        :param must_include: a list of database IDs of objects which are to be included
          even if the thinning criteria would otherwise discard them.
        """
        self.base_object = base_object

        # NB the defaults are resolved here, rather than in the signature, so that changes
        # to the module-level configuration are picked up by trees constructed afterwards
        self._min_fractional_weight = mergertree_min_fractional_weight \
            if min_fractional_weight is None else min_fractional_weight
        self._min_fractional_NDM = mergertree_min_fractional_NDM \
            if min_fractional_NDM is None else min_fractional_NDM
        self._max_nobjects = mergertree_max_nhalos if max_nobjects is None else max_nobjects
        self._max_hops = mergertree_max_hops if max_hops is None else max_hops
        self._timeout = timeout
        self._must_include = [] if must_include is None else list(must_include)

        self._root = None
        self._nodes = None
        self._objects = None
        self._node_from_object_id = None
        self._timestep_groups = None
        self._truncated = False
        self._link_cache = None
        self._construction_start_time = None

    def construct(self, force=False):
        """Construct the tree, if it has not already been constructed.

        It is not normally necessary to call this: the tree is built automatically when it
        is first used. Calling it explicitly is useful only to control when the database
        queries take place.

        :param force: if True, rebuild the tree even if it has already been constructed
        """
        if self._nodes is not None and not force:
            return

        # NB the timeout covers the database query as well as the tree construction, since
        # for a large tree the query is usually the more expensive of the two
        self._construction_start_time = time.time()
        self._generate_link_cache()
        self._build_nodes()
        self._link_cache = None # no longer needed, and potentially large

    def _ensure_constructed(self):
        if self._nodes is None:
            self.construct()

    @property
    def truncated(self):
        """True if construction was abandoned early because :attr:`timeout` was exceeded.

        A truncated tree is a valid tree, but some branches stop before reaching their
        earliest progenitor.
        """
        self._ensure_constructed()
        return self._truncated

    # ------------------------------------------------------------------
    # construction
    # ------------------------------------------------------------------

    def _generate_link_cache(self):
        """Query the database for the links making up the tree, and index them by source"""
        strategy = MultiHopAllProgenitorsStrategy(self.base_object, nhops_max=self._max_hops)
        self._link_cache = {}
        for link in strategy._get_query_all():
            self._link_cache.setdefault(link.halo_from_id, []).append(link)

    def _build_nodes(self):
        """Turn the link cache into a tree of TreeNodes, applying the thinning criteria"""
        root = TreeNode(self.base_object, None, 0, 1.0)

        # Each object appears at most once in the tree; the strategy combines routes, so
        # duplicates should not arise, but tracking them explicitly guarantees the
        # 'every object exactly once' promise made by walk_depth.
        visited = {self.base_object.id}

        must_include = set(self._must_include)
        self._truncated = False

        current_level = [root]
        while len(current_level) > 0:
            if self._timeout is not None and \
                    time.time() - self._construction_start_time > self._timeout:
                self._truncated = True
                logger.warning("Merger tree construction timed out after %.1fs; "
                               "the tree returned is incomplete", self._timeout)
                break

            links = []
            for node in current_level:
                links += self._link_cache.get(node.obj.id, [])
            if len(links) == 0:
                break

            kept_links_by_source = {}
            for link in self._select_links(links, visited, must_include):
                kept_links_by_source.setdefault(link.halo_from_id, []).append(link)

            next_level = []
            for node in current_level:
                for link in kept_links_by_source.get(node.obj.id, []):
                    child = TreeNode(link.halo_to, node, node.depth + 1, link.weight)
                    node.children.append(child)
                    next_level.append(child)

            current_level = next_level

        self._order_nodes(root)

    def _select_links(self, links, visited, must_include):
        """Apply the thinning criteria to all the links leading out of one depth of the tree

        :param links: the candidate links, which must all originate at the same depth
        :param visited: set of database IDs already in the tree; updated in place
        :param must_include: set of database IDs exempt from thinning
        :returns: the subset of links to follow, in the order given
        """
        NDM = [link.halo_to.NDM for link in links]
        max_NDM = max(NDM)

        if len(links) > self._max_nobjects:
            NDM_cut = sorted(NDM)[-self._max_nobjects]
        else:
            NDM_cut = None

        max_weight_from = {}
        for link in links:
            if link.weight > max_weight_from.get(link.halo_from_id, -np.inf):
                max_weight_from[link.halo_from_id] = link.weight

        kept = []
        for link in links:
            include = link.weight > max_weight_from[link.halo_from_id] * self._min_fractional_weight
            include = include and ((link.halo_to.NDM > self._min_fractional_NDM * max_NDM)
                                   or (link.halo_to.NDM == 0))
            if NDM_cut is not None:
                include = include and (link.halo_to.NDM > NDM_cut)

            if link.halo_to_id in must_include:
                include = True # override the normal criteria
            if link.halo_to_id in visited:
                include = False # already reached by another route

            if include:
                kept.append(link)
                visited.add(link.halo_to_id)

        return kept

    def _order_nodes(self, root):
        """Sort each node's progenitors, then flatten the tree into canonical order

        The canonical order is depth-first pre-order, with progenitors visited in order of
        decreasing link weight (i.e. major progenitor first), ties being broken by object
        number. Consequently the first branch of the tree is its main branch, and at any
        timestep reached by the main branch, the main branch object has the lowest index.
        """
        self._nodes = []
        stack = [root]
        while len(stack) > 0:
            node = stack.pop()
            node.index = len(self._nodes)
            self._nodes.append(node)
            node.children.sort(key=lambda child: (-child.weight, child.obj.halo_number,
                                                  child.obj.id))
            stack += reversed(node.children)

        self._root = root
        self._objects = [node.obj for node in self._nodes]
        self._node_from_object_id = {node.obj.id: node for node in self._nodes}
        self._timestep_groups = None

    # ------------------------------------------------------------------
    # structure
    # ------------------------------------------------------------------

    @property
    def objects(self):
        """All objects in the tree, in canonical (depth-first, major progenitor first) order.

        The arrays returned by :meth:`calculate_all` are aligned with this list.
        """
        self._ensure_constructed()
        return list(self._objects)

    @property
    def timesteps(self):
        """The timesteps spanned by the tree, ordered from latest to earliest.

        Timesteps containing no object in the tree are omitted, so this list is in 1-1
        correspondence with the groups yielded by :meth:`walk_depth`.
        """
        self._ensure_constructed()
        return [timestep for timestep, _ in self._get_timestep_groups()]

    def __len__(self):
        """The number of objects in the tree"""
        self._ensure_constructed()
        return len(self._nodes)

    def __iter__(self):
        """Iterate over all objects in the tree, in canonical order"""
        self._ensure_constructed()
        return iter(self._objects)

    def __contains__(self, obj):
        """True if the specified object is in the tree"""
        self._ensure_constructed()
        return obj.id in self._node_from_object_id

    def _node(self, obj):
        """Return the :class:`TreeNode` occupied by the specified object.

        :raises ValueError: if the object is not in the tree
        """
        self._ensure_constructed()
        try:
            return self._node_from_object_id[obj.id]
        except KeyError:
            raise ValueError(f"{obj!r} is not in this merger tree") from None

    def index(self, obj):
        """Return the canonical index of the specified object.

        This is the position of the object within :attr:`objects`, and therefore also the
        position of its result within the arrays returned by :meth:`calculate_all`.

        :raises ValueError: if the object is not in the tree
        """
        return self._node(obj).index

    def depth(self, obj):
        """Return the number of links separating the specified object from the base object.

        Note that this is a number of links, not of timesteps: if the database contains
        links which skip over a timestep, two objects at the same timestep can be at
        different depths.

        :raises ValueError: if the object is not in the tree
        """
        return self._node(obj).depth

    def weight(self, obj):
        """Return the cumulative weight of the route from the base object to this object.

        The cumulative weight is the product of the weights of the individual links along
        the route; the base object itself has weight 1. Link weights are written by the
        linking code and their normalisation is therefore convention-dependent, but they
        increase with the strength of the association, so that the major progenitor of an
        object has a larger weight than its minor progenitors.

        :raises ValueError: if the object is not in the tree
        """
        return self._node(obj).weight

    def descendant(self, obj):
        """Return the object into which the specified object merges, within this tree.

        Returns None for the base object. Note that thinning may mean the result differs
        from ``obj.next``.

        :raises ValueError: if the object is not in the tree
        """
        node = self._node(obj)
        return None if node.parent is None else node.parent.obj

    def progenitors(self, obj):
        """Return the progenitors of the specified object which are in this tree.

        The list is ordered with the major progenitor first, and is empty for objects at
        the tips of branches. Note that thinning may mean the result contains fewer objects
        than ``obj.previous``-style navigation would suggest.

        :raises ValueError: if the object is not in the tree
        """
        return [child.obj for child in self._node(obj).children]

    # ------------------------------------------------------------------
    # properties
    # ------------------------------------------------------------------

    def calculate_all(self, *properties):
        """Evaluate the specified properties or live calculations for every object in the tree.

        For example:

        >>> Mvir, Rvir = tree.calculate_all(lambda: Mvir, lambda: Rvir)

        Each property may be given as a lambda taking no arguments, as a string, or as a
        :class:`tangos.live_calculation.Calculation` object, exactly as elsewhere in tangos.

        The returned arrays are aligned with :attr:`objects`: element ``i`` of each array
        refers to ``tree.objects[i]``. Unlike
        :meth:`tangos.core.timestep.TimeStep.calculate_all`, rows for which no result could
        be obtained are therefore *not* dropped, since that would break the alignment. Such
        a gap is marked by NaN in a column of floating point numbers, and by None
        otherwise, in which case the column is an object array — the same representation
        that ``TimeStep.calculate_all(..., sanitize=False)`` uses.

        The arrays may be passed to :meth:`walk_depth` or :meth:`walk_branches`, which slice
        them to match the objects they yield.

        :returns: a list with one array per property, each of length ``len(tree)``
        """
        self._ensure_constructed()

        if len(properties) == 0:
            return []

        # the database id is prepended to the calculation, so that the results, which come
        # back in an order determined by the query, can be matched up with the tree
        calculation = live_calculation.MultiCalculation(
            live_calculation.LiveProperty("dbid"),
            live_calculation.parse_property_list(*properties))
        n_columns = calculation.n_columns() - 1

        object_ids = [obj.id for obj in self._objects]

        @contextlib.contextmanager
        def objects_in_this_tree(session):
            with temporary_halolist.temporary_halolist_table(session, object_ids) as temptable:
                yield temporary_halolist.halo_query(temptable), None

        results = live_calculation.calculate_over_objects(
            calculation, objects_in_this_tree, object_session(self.base_object),
            sanitize=False)

        results_from_object_id = {row[0]: row[1:] for row in results.T}
        no_result = [None] * n_columns
        rows = [results_from_object_id.get(object_id, no_result) for object_id in object_ids]

        return [live_calculation.values_to_array([row[i] for row in rows], mark_gaps=True)
                for i in range(n_columns)]

    # ------------------------------------------------------------------
    # walking
    # ------------------------------------------------------------------

    def _get_timestep_groups(self):
        """Return a list of (timestep, indices) pairs, ordered from latest to earliest"""
        if self._timestep_groups is None:
            groups = {}
            for node in self._nodes:
                _, indices = groups.setdefault(node.obj.timestep_id,
                                               (node.obj.timestep, []))
                indices.append(node.index)
            self._timestep_groups = sorted(groups.values(),
                                           key=lambda group: group[0].time_gyr,
                                           reverse=True)
        return self._timestep_groups

    def _gather(self, indices, properties):
        objects = [self._objects[i] for i in indices]
        if len(properties) == 0:
            return objects
        return (objects,) + tuple(_take(p, indices) for p in properties)

    def walk_depth(self, *properties, reverse=False):
        """Walk over the tree one timestep at a time.

        Each iteration yields the objects in the tree belonging to a single timestep, so
        that every object in the tree is yielded exactly once over the course of the walk,
        and within an iteration all objects share a timestep. Timesteps are visited from
        latest to earliest, unless ``reverse`` is True. Within an iteration, objects are in
        canonical order, so that the main branch object (where the main branch reaches this
        timestep) comes first.

        Any arrays passed in, typically obtained from :meth:`calculate_all`, are sliced to
        match the objects being yielded:

        >>> Mvir, = tree.calculate_all(lambda: Mvir)
        >>> for objects, Mvir_now in tree.walk_depth(Mvir):
        ...     print(objects[0].timestep.redshift, np.nansum(Mvir_now))

        With no arrays passed in, each iteration yields the list of objects alone rather
        than a one-element tuple:

        >>> for objects in tree.walk_depth():
        ...     print(len(objects))

        :param properties: arrays indexed like :attr:`objects`, to be sliced alongside it
        :param reverse: if True, walk from the earliest timestep to the latest
        """
        self._ensure_constructed()
        groups = self._get_timestep_groups()
        for _, indices in (reversed(groups) if reverse else groups):
            yield self._gather(indices, properties)

    def walk_branches(self, *properties):
        """Walk over the tree one branch at a time.

        Each iteration yields a single branch: a chain of objects running from latest to
        earliest, in which each object is the major progenitor of the one before it. No two
        objects in an iteration therefore share a timestep.

        The first branch yielded is the main branch, starting at the base object. Every
        subsequent branch starts with an object which has already been yielded — the
        descendant into which the branch merges — thereby marking where the branch diverges
        from a part of the tree already seen. Excluding those repeated first objects, every
        object in the tree is yielded exactly once.

        Any arrays passed in, typically obtained from :meth:`calculate_all`, are sliced to
        match the objects being yielded. This makes it straightforward to plot a tree:

        >>> Mvir, = tree.calculate_all(lambda: Mvir)
        >>> for objects, Mvir_branch in tree.walk_branches(Mvir):
        ...     plt.plot([obj.timestep.redshift for obj in objects], Mvir_branch)

        With no arrays passed in, each iteration yields the list of objects alone rather
        than a one-element tuple.

        :param properties: arrays indexed like :attr:`objects`, to be sliced alongside it
        """
        self._ensure_constructed()
        for node in self._nodes:
            if node.parent is not None and node.parent.children[0] is node:
                continue # not the start of a new branch; it continues its parent's branch

            indices = [] if node.parent is None else [node.parent.index]
            while node is not None:
                indices.append(node.index)
                node = node.children[0] if len(node.children) > 0 else None

            yield self._gather(indices, properties)

    # ------------------------------------------------------------------
    # display
    # ------------------------------------------------------------------

    def plot(self, **kwargs):
        """Display the tree using matplotlib"""
        MergerTreeLayout(self, **kwargs).plot()

    def summarise(self, node=None):
        """Generate a string summarising the tree structure.

        This is implemented for debugging purposes and is not particularly easy to read.
        For a human-readable summary of a tree, simply call ``str(tree)``.

        :param node: the :class:`TreeNode` to summarise from; defaults to the base object
        """
        self._ensure_constructed()
        if node is None:
            node = self._root
        result = str(node.obj.halo_number)
        children = ",".join(self.summarise(child) for child in node.children)
        if len(children) > 0:
            result += "(" + children + ")"
        return result

    def __str__(self):
        """Generate a human-readable multi-line string displaying the tree structure"""
        return str(MergerTreeLayout(self))

    def __repr__(self):
        if self._nodes is None:
            return f"<MergerTree from {self.base_object!r} (not yet constructed)>"
        return "<MergerTree from %r with %d objects, maximum depth %d>" % (
            self.base_object, len(self._nodes), max(node.depth for node in self._nodes))


def _take(values, indices):
    """Select the specified indices from an array or list, preserving its type where possible"""
    if isinstance(values, np.ndarray):
        return values[indices]
    return [values[i] for i in indices]


class MergerTreeLayout:
    """A two-dimensional layout of a merger tree, for plotting or display.

    The layout is expressed as a nested dictionary, one entry per object, with progenitors
    stored under the key ``contents``. Each entry carries a horizontal position ``_x`` and
    a radius ``size``, chosen so that branches do not cross and so that more massive
    objects are drawn larger.

    Subclasses may override :meth:`node_dict` to attach further information to each entry;
    this is how the web interface adds the labels and links it needs.
    """

    def __init__(self, tree, x_step=5):
        """Set up a layout for the specified tree, constructing the tree if necessary.

        :param tree: the tree to lay out
        :type tree: MergerTree
        :param x_step: the horizontal spacing between objects
        """
        self.tree = tree
        tree.construct()
        self.x_step = x_step
        self._treedata = None

    def as_dict(self):
        """Return the layout as a nested dictionary, with the base object at the top level"""
        if self._treedata is None:
            max_depth = max(node.depth for node in self.tree._nodes)
            self._treedata = self._build(self.tree._root, max_depth)
            self._postprocess()
        return self._treedata

    def _build(self, node, max_depth):
        entry = self.node_dict(node)
        entry['contents'] = [self._build(child, max_depth) for child in node.children]
        # NB maxdepth counts the number of levels in the tree at or below this one, and is
        # therefore the same for every object at a given depth
        entry['maxdepth'] = max_depth - node.depth + 1
        return entry

    def node_dict(self, node):
        """Return the layout dictionary describing a single object.

        :param node: the tree node to describe
        :type node: TreeNode
        """
        obj = node.obj

        name = str(obj.halo_number)
        if len(name) > 4:
            name = ""

        if obj.NDM > 0:
            unscaled_size = math.log10(obj.NDM)
        else:
            unscaled_size = 1

        return {'name': name,
                'halo_number': obj.halo_number,
                'unscaled_size': unscaled_size,
                'contents': [],
                'depth': node.depth,
                'halo_number_with_phantom_offset': obj.halo_number +
                        10000 * isinstance(obj, core.halo.PhantomHalo)}

    def plot(self):
        """Display the tree in matplotlib"""
        import pylab as p
        ax = p.gca()
        for node in self._visit_tree():
            y = node['depth']*self.x_step
            x = node['_x']
            circ = p.Circle((x,y),node['size'],facecolor='black',edgecolor='none')
            ax.add_patch(circ)
            ax.text(x+node['size']*1.1,y,node['name'])
            for child_node in node['contents']:
                p.plot([node['_x'],child_node['_x']],[y,y+self.x_step],'k')
        ax.set_aspect('equal','datalim')

    def __str__(self):
        """Generate a human-readable multi-line string displaying the tree structure"""
        result = ""
        for layer in self._visit_tree_layers():
            max_x = max(node['_x'] for node in layer)
            this_layer_stubs = this_layer_connections = this_layer_string = " "*(max_x+self.x_step)
            need_connections = False
            for node in layer:
                x0 = node['_x']
                x1 = x0+self.x_step
                xcen = x0+self.x_step//2
                node_string = str(node['halo_number']).center(self.x_step)
                this_layer_string=this_layer_string[:x0]+node_string+this_layer_string[x1:]

                if 'contents' in node and len(node['contents'])>0 :
                    connector = "|"

                    x0_min_next_layer = min(child_node['_x'] for child_node in node['contents'])
                    x0_max_next_layer = max(child_node['_x'] for child_node in node['contents'])
                    line_start = x0_min_next_layer+self.x_step//2
                    line_finish = x0_max_next_layer+self.x_step//2

                    if line_finish<xcen:
                        connector = "/"
                        xcen-=1
                    elif line_start>xcen:
                        connector = "\\"
                        xcen+=1
                    this_layer_stubs = this_layer_stubs[:xcen] + connector + this_layer_stubs[xcen + 1:]
                    if line_finish!=line_start:
                        line = "-"*(line_finish-line_start+1)
                        this_layer_connections = this_layer_connections[:line_start] + line +\
                                                 this_layer_connections[line_finish+1:]
                        need_connections = True
                    else:
                        this_layer_connections = this_layer_connections[:line_start] + "|" + this_layer_connections[line_start+1:]

            result+=this_layer_string+"\r\n"+this_layer_stubs+"\r\n"
            if need_connections:
                result+=this_layer_connections+"\r\n"
        return result

    def _visit_tree(self, tree=None):
        """Yields each layout entry in turn"""
        if tree is None:
            tree = self.as_dict()
        yield tree
        for subtree in tree['contents']:
            yield from self._visit_tree(subtree)

    def _visit_tree_layers(self):
        """Yields each layer of the tree in turn, consisting of a list of entries at each layer"""
        layers = self._get_tree_layers()
        for i in range(len(layers)):
            yield layers[i]

    def _get_tree_layers(self):
        layers = [[] for i in range(self.as_dict()['maxdepth'])]
        for leaf in self._visit_tree():
            this_depth = leaf['depth']
            layers[this_depth].append(leaf)
        return layers

    def _postprocess(self):
        """Once all entries are present, re-process them into a sensible layout"""
        self._postprocess_rescale()
        self._postprocess_layout_by_branch()

    def _postprocess_rescale(self):
        max_size = -100
        for node in self._visit_tree(self._treedata):
            if node['unscaled_size'] > max_size:
                max_size = node['unscaled_size']

        for node in self._visit_tree(self._treedata):
            size = self.x_step/3 + (self.x_step/10) * (node['unscaled_size'] - max_size)
            if size < self.x_step/10:
                size = self.x_step/10
            node['size'] = size

    def _postprocess_layout_by_branch(self):
        """Associate with each entry a topological spatial range such that branches do not cross"""
        self._treedata['space_range'] = (0.0, 1.0)
        existing_ranges = [{} for i in range(self._treedata['maxdepth'])]
        for node in self._visit_tree(self._treedata):
            x_start, x_end = node['space_range']
            node['mid_range'] = (x_start + x_end) / 2
            if len(node['contents']) > 0:
                delta = (x_end - x_start) / len(node['contents'])
                total_nodes = len(node['contents'])
                halo_numbers = [child['halo_number_with_phantom_offset'] for child in node['contents']]
                halo_numbers.sort()
                for i, child in enumerate(node['contents']):
                    # create an index that starts in the middle then works outwards
                    rank = halo_numbers.index(child['halo_number_with_phantom_offset'])
                    sign = 2 * ((rank + 1) % 2) - 1
                    i_shuffled = total_nodes // 2 + sign * ((rank + 1) // 2)
                    child_range = existing_ranges[child['depth']].get(child['halo_number_with_phantom_offset'],
                                                                      (x_start + i_shuffled * delta,
                                                                       x_start + (i_shuffled + 1) * delta))

                    child['space_range'] = child_range
                    existing_ranges[child['depth']][child['halo_number_with_phantom_offset']] = child_range

        self._postprocess_layout_by_number('mid_range')

    def _postprocess_layout_by_number(self, key='halo_number'):
        """Associate a position value _x to each entry that increases with the specified key

        The _x value is guaranteed to increase with increasing key value but also will be centred on zero"""
        x_vals = [set() for i in range(self._treedata['maxdepth'])]

        for node in self._visit_tree(self._treedata):
            x_vals[node['depth']].add(node[key])

        max_entries = max(len(v) for v in x_vals)
        x_map = [{} for i in range(self._treedata['maxdepth'])]
        for this_vals, this_map in zip(x_vals, x_map):
            new_x = (self.x_step//2) * (max_entries - len(this_vals))
            for xv in sorted(this_vals):
                this_map[xv] = new_x
                new_x += self.x_step

        for node in self._visit_tree(self._treedata):
            node['_x'] = x_map[node['depth']][node[key]]
