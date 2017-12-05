from __future__ import division
from ..config import mergertree_timeout, mergertree_max_nhalos, mergertree_min_fractional_NDM, mergertree_min_fractional_weight
from . import MultiSourceMultiHopStrategy
from ..util import consistent_collection as cc
from .. import core
import time, math
from six.moves import range
from six.moves import zip


class MergerTree(object):
    """Construct a merger tree from a given starting halo.

    This is chiefly for use by the web interface, since displaying the resulting data
    is non-trivial. However, it is also possible to use MergerTree from an interactive
    python session as follows:

     halo = tangos.get_halo(...)
     tree = tangos.relation_finding.tree.MergerTree(halo)
     tree.construct()
     tree.plot()

    This will display the tree information in matplotlib."""

    def __init__(self, base_halo):
        """Initialise the tree starting at the specified base halo.

        Note that the method construct() must be called to actually build the tree"""
        self.base_halo = base_halo
        self.highlight_halo = base_halo
        self.timeout = mergertree_timeout
        self.must_include = []
        self.x_step = 5

    def construct(self):
        """Construct the tree"""
        self._construction_start_time = time.time()
        self._treedata = self._construct_preliminary([self.base_halo])[0]
        self._postprocess()

    def _construct_preliminary(self, halos, depth=0):
        """Construct a preliminary representation of the tree, which will later be revised
        by the post-processing"""
        if len(halos) == 0:
            return []

        visited = []

        output = [self._get_basic_halo_node(halo, depth) for halo in halos]

        previous_timestep = cc.ConsistentCollection(halos).timestep.previous
        if previous_timestep is not None:

            rl = MultiSourceMultiHopStrategy(halos,target=previous_timestep, nhops_max=1,
                                             one_match_per_input=False)

            link_objs = rl._get_query_all()
        else:
            link_objs = []

        pairings = []
        next_level_halos = []
        next_level_halos_link_from = []

        maxdepth = 0

        if len(link_objs) > 0 and time.time() - self._construction_start_time < self.timeout:
            max_weight = max([o.weight for o in link_objs])
            NDM_array = [o.halo_to.NDM for o in link_objs]
            max_NDM = max(NDM_array)
            if len(NDM_array) > mergertree_max_nhalos:
                NDM_cut = sorted(NDM_array)[mergertree_max_nhalos]
            else:
                NDM_cut = None

            for obj in link_objs:
                should_construct_onward_tree = obj.weight > max_weight * mergertree_min_fractional_weight
                should_construct_onward_tree &= obj.halo_to.NDM > mergertree_min_fractional_NDM * max_NDM
                if NDM_cut:
                    should_construct_onward_tree &= obj.halo_to.NDM > NDM_cut

                if obj.halo_to_id in self.must_include:
                    should_construct_onward_tree = True  # override normal criteria
                if obj.halo_to_id in visited:
                    should_construct_onward_tree = False  # already expanded via another link

                if should_construct_onward_tree:
                    next_level_halos.append(obj.halo_to)
                    next_level_halos_link_from.append(halos.index(obj.halo_from))
                    visited.append(obj.halo_to_id)

            next_level_items = self._construct_preliminary(next_level_halos,  depth + 1)

            for placement_id, item in zip(next_level_halos_link_from, next_level_items):
                output[placement_id]['contents'].append(item)
                if item['maxdepth'] > maxdepth:
                    maxdepth = item['maxdepth']

        for i in output:
            i['maxdepth'] = maxdepth + 1

        return output


    def _get_basic_halo_node(self, halo, depth):
        """Get the dictionary of properties belonging to a node of the tree, i.e. a given halo"""
        timeinfo = "TS ...%s; z=%.2f; t=%.2e Gyr" % (
            halo.timestep.extension[-5:], halo.timestep.redshift, halo.timestep.time_gyr)

        if halo.NDM > 0:
            moreinfo = "%s %d, NDM=%.2e" % (halo.__class__.__name__, halo.halo_number, halo.NDM)
            unscaled_size = math.log10(halo.NDM)
        else:
            moreinfo = "%s %d" % (halo.__class__.__name__, halo.halo_number)
            unscaled_size = 1

        try:
            Mvir = halo.properties.filter_by(
                name_id=core.dictionary.get_dict_id("Mvir", session=core.Session.object_session(halo),
                                                           allow_query=False)).first()
        except KeyError:
            Mvir = None
        if Mvir is not None:
            moreinfo += ", %s=%.2e" % ("Mvir", Mvir.data)

        nodeclass = 'node-dot-standard'
        name = str(halo.halo_number)

        if halo == self.highlight_halo:
            nodeclass = 'node-dot-selected'
        elif depth == 0:
            if halo.next is not None:
                nodeclass = 'node-dot-continuation'
                name = '...'
                moreinfo = "Continues... " + moreinfo
        if len(name) > 4:
            name = ""
        output = {'name': name,
                  'nodeclass': nodeclass,
                  'moreinfo': moreinfo,
                  'timeinfo': timeinfo,
                  'halo_number': halo.halo_number,
                  'unscaled_size': unscaled_size,
                  'contents': [],
                  'depth': depth}
        return output

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
        result = ""
        for layer in self._visit_tree_layers():
            max_x = max([node['_x'] for node in layer])
            this_layer_stubs = this_layer_connections = this_layer_string = " "*(max_x+self.x_step)
            for node in layer:
                x0 = node['_x']
                x1 = x0+self.x_step
                node_string = str(node['halo_number']).center(self.x_step)
                this_layer_string=this_layer_string[:x0]+node_string+this_layer_string[x1:]

                if 'contents' in node and len(node['contents'])>0 :
                    this_layer_stubs = this_layer_stubs[:x0+self.x_step//2]+"|"+this_layer_stubs[x0+self.x_step//2:]
                    x0_min_next_layer = min([child_node['_x'] for child_node in node['contents']])
                    x0_max_next_layer = max([child_node['_x'] for child_node in node['contents']])
                    line_start = x0_min_next_layer+self.x_step//2
                    line_finish = x0_max_next_layer+self.x_step//2
                    line = "-"*(line_finish-line_start)
                    this_layer_connections = this_layer_connections[:line_start] + line +\
                                             this_layer_connections[line_finish:]

            result+=this_layer_string+"\r\n"+this_layer_stubs+"\r\n"+this_layer_connections+"\r\n"
        return result


    def _visit_tree(self, tree=None):
        """Yields each tree element in turn"""
        if tree is None:
            tree = self._treedata
        yield tree
        for subtree in tree['contents']:
            for item in self._visit_tree(subtree):
                yield item

    def _visit_tree_layers(self):
        """Yields each layer of the tree in turn, consisting of a list of elements at each layer"""
        layers = {}
        for leaf in self._visit_tree():
            this_depth = leaf['depth']
            if this_depth not in layers:
                layers[this_depth] = []
            layers[this_depth].append(leaf)
        for i in range(self._treedata['maxdepth']):
            yield layers[i]


    def _postprocess_megertree_rescale(self):
        max_size = -100
        for node in self._visit_tree():
            if node['unscaled_size'] > max_size:
                max_size = node['unscaled_size']

        for node in self._visit_tree():
            size = self.x_step/3 + (self.x_step/10) * (node['unscaled_size'] - max_size)
            if size < self.x_step/10:
                size = self.x_step/10
            node['size'] = size

    def _postprocess(self):
        """Once all tree information is present, re-process the nodes to a more sensible layout"""
        self._postprocess_megertree_rescale()
        self._postprocess_mergertree_layout_by_branch()

    def _postprocess_mergertree_layout_by_branch(self):
        """Associate with each tree node a toplogical spatial range such that branches do not cross"""
        self._treedata['space_range'] = (0.0, 1.0)
        existing_ranges = [{} for i in range(self._treedata['maxdepth'])]
        for node in self._visit_tree():
            x_start, x_end = node['space_range']
            node['mid_range'] = (x_start + x_end) / 2
            if len(node['contents']) > 0:
                delta = (x_end - x_start) / len(node['contents'])
                total_nodes = len(node['contents'])
                halo_numbers = [child['halo_number'] for child in node['contents']]
                halo_numbers.sort()
                for i, child in enumerate(node['contents']):
                    # create an index that starts in the middle then works outwards
                    rank = halo_numbers.index(child['halo_number'])
                    sign = 2 * ((rank + 1) % 2) - 1
                    i_shuffled = total_nodes // 2 + sign * ((rank + 1) // 2)
                    child_range = existing_ranges[child['depth']].get(child['halo_number'],
                                                                      (x_start + i_shuffled * delta,
                                                                       x_start + (i_shuffled + 1) * delta))

                    child['space_range'] = child_range
                    existing_ranges[child['depth']][child['halo_number']] = child_range

        self._postprocess_mergertree_layout_by_number('mid_range')

    def _postprocess_mergertree_layout_by_number(self, key='halo_number'):
        """Associate a position value _x to each node that increases with the specified key

        The _x value is guaranteed to increase with increasing key value but also will be centred on zero"""
        x_vals = [set() for i in range(self._treedata['maxdepth'])]

        for node in self._visit_tree():
            x_vals[node['depth']].add(node[key])

        max_entries = max([len(v) for v in x_vals])
        x_map = [{} for i in range(self._treedata['maxdepth'])]
        for this_vals, this_map in zip(x_vals, x_map):
            new_x = (self.x_step//2) * (max_entries - len(this_vals))
            for xv in sorted(this_vals):
                this_map[xv] = new_x
                new_x += self.x_step

        for node in self._visit_tree():
            node['_x'] = x_map[node['depth']][node[key]]
