from __future__ import absolute_import
from __future__ import print_function
from pyramid.view import view_config
from . import halo_from_request
import tangos, tangos.relation_finding, tangos.util.consistent_collection as cc
import time
import math
from six.moves import range
from six.moves import zip
from ...config import mergertree_min_fractional_NDM, mergertree_min_fractional_weight, mergertree_timeout, mergertree_max_nhalos

def _construct_preliminary_mergertree(halos, base_halo, must_include, request, depth=0):


    if len(halos)==0:
        return []

    visited = []

    output = [_get_basic_halo_node(halo, base_halo, depth, request) for halo in halos]


    rl = tangos.relation_finding.MultiSourceMultiHopStrategy(halos,
                                                             target=cc.ConsistentCollection(halos).timestep.previous,
                                                             nhops_max=1,
                                                             one_match_per_input=False)

    link_objs = rl._get_query_all()
    pairings = []
    next_level_halos = []
    next_level_halos_link_from = []

    maxdepth = 0

    if len(link_objs)>0 and time.time()-request.start_time<mergertree_timeout:
        max_weight = max([o.weight for o in link_objs])
        NDM_array = [o.halo_to.NDM for o in link_objs]
        max_NDM = max(NDM_array)
        if len(NDM_array)>mergertree_max_nhalos:
            NDM_cut = sorted(NDM_array)[mergertree_max_nhalos]
        else:
            NDM_cut = None

        for obj in link_objs:
            should_construct_onward_tree = obj.weight>max_weight*mergertree_min_fractional_weight
            should_construct_onward_tree&= obj.halo_to.NDM>mergertree_min_fractional_NDM*max_NDM
            if NDM_cut:
                should_construct_onward_tree&=obj.halo_to.NDM>NDM_cut

            if obj.halo_to_id in must_include:
                should_construct_onward_tree = True # override normal criteria
            if obj.halo_to_id  in visited:
                should_construct_onward_tree = False # already expanded via another link

            if should_construct_onward_tree:
                next_level_halos.append(obj.halo_to)
                next_level_halos_link_from.append(halos.index(obj.halo_from))
                visited.append(obj.halo_to_id)

        next_level_items = _construct_preliminary_mergertree(next_level_halos, base_halo, must_include, request, depth+1)


        for placement_id, item in zip(next_level_halos_link_from, next_level_items):
            output[placement_id]['contents'].append(item)
            if item['maxdepth']>maxdepth:
                maxdepth = item['maxdepth']

    for i in output:
        i['maxdepth'] = maxdepth+1

    return output



def _get_basic_halo_node(halo, base_halo, depth,  request):
    timeinfo = "TS ...%s; z=%.2f; t=%.2e Gyr" % (
        halo.timestep.extension[-5:], halo.timestep.redshift, halo.timestep.time_gyr)

    if halo.NDM > 0:
        moreinfo = "%s %d, NDM=%.2e" % (halo.__class__.__name__, halo.halo_number, halo.NDM)
        unscaled_size = math.log10(halo.NDM)
    else:
        moreinfo = "%s %d" % (halo.__class__.__name__, halo.halo_number, halo.NDM)
        unscaled_size = 1

    try:
        Mvir = halo.properties.filter_by(
            name_id=tangos.core.dictionary.get_dict_id("Mvir", session=tangos.core.Session.object_session(halo),
                                                       allow_query=False)).first()
    except KeyError:
        Mvir = None
    if Mvir is not None:
        moreinfo += ", %s=%.2e" % ("Mvir", Mvir.data)

    nodeclass = 'node-dot-standard'
    name = str(halo.halo_number)
    #if halo.links.filter_by(relation_id=tangos.core.dictionary.get_dict_id('Sub', -1, session=tangos.core.Session.object_session(halo), allow_query=False)).count() > 0:
    #    nodeclass = 'node-dot-sub'
    if halo == base_halo:
        nodeclass = 'node-dot-selected'
    elif depth == 0:
        if halo.next is not None:
            nodeclass = 'node-dot-continuation'
            name = '...'
            moreinfo = "Continues... " + moreinfo
    if len(name) > 4:
        name = ""
    output = {'name': name,
              'url': request.route_url('halo_view', simid=base_halo.timestep.simulation.basename,
                                       timestepid=halo.timestep.extension,
                                       halonumber=halo.basename),
              'nodeclass': nodeclass,
              'moreinfo': moreinfo,
              'timeinfo': timeinfo,
              'halo_number': halo.halo_number,
              'unscaled_size': unscaled_size,
              'contents': [],
              'depth': depth}
    return output


def _visit_tree(tree):
    yield tree
    for subtree in tree['contents']:
        for item in _visit_tree(subtree): yield item


def _postprocess_megertree_rescale(tree):
    max_size = -100
    for node in _visit_tree(tree):
        if node['unscaled_size'] > max_size:
            max_size = node['unscaled_size']

    for node in _visit_tree(tree):
        size = 10 + 3 * (node['unscaled_size'] - max_size)
        if size < 3:
            size = 3
        node['size'] = size


def _postprocess_mergertree(tree):
    _postprocess_megertree_rescale(tree)
    _postprocess_mergertree_layout_by_branch(tree)


def _postprocess_mergertree_layout_by_number(tree, key='halo_number'):
    x_vals = [set() for i in range(tree['maxdepth'])]

    for node in _visit_tree(tree):
        x_vals[node['depth']].add(node[key])

    max_entries = max([len(v) for v in x_vals])
    x_map = [{} for i in range(tree['maxdepth'])]
    for this_vals, this_map in zip(x_vals, x_map):
        new_x = 15 * (max_entries - len(this_vals))
        for xv in sorted(this_vals):
            this_map[xv] = new_x
            new_x += 30

    for node in _visit_tree(tree):
        node['_x'] = x_map[node['depth']][node[key]]


def _postprocess_mergertree_layout_by_branch(tree):
    tree['space_range'] = (0.0, 1.0)
    existing_ranges = [{} for i in range(tree['maxdepth'])]
    for node in _visit_tree(tree):
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
                sign = 2*((rank+1)%2)-1
                i_shuffled = total_nodes//2 + sign*((rank+1)//2)
                child_range = existing_ranges[child['depth']].get(child['halo_number'],
                                                                  (x_start + i_shuffled * delta, x_start + (i_shuffled + 1) * delta))

                child['space_range'] = child_range
                existing_ranges[child['depth']][child['halo_number']] = child_range

    _postprocess_mergertree_layout_by_number(tree, 'mid_range')


def _construct_mergertree(halo, request):
    search_time = 0
    request.start_time = time.time()
    base = halo
    must_include = []
    for i in range(5):
        must_include.append(base.id)
        if base.next is not None:
            base = base.next

    tree = _construct_preliminary_mergertree([base], halo, must_include, request)[0]
    print("Merger tree build time:    %.2fs" % (time.time() - request.start_time))

    start = time.time()
    _postprocess_mergertree(tree)
    print("Post-processing time: %.2fs" % (time.time() - start))


    return tree

@view_config(route_name='merger_tree', renderer='json')
def merger_tree(request):
    halo = halo_from_request(request)

    return {'tree': _construct_mergertree(halo, request)}