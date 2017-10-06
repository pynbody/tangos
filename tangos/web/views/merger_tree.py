from __future__ import absolute_import
from __future__ import print_function
from pyramid.view import view_config
from . import halo_from_request
import tangos
import time
import math
from six.moves import range
from six.moves import zip

def _construct_preliminary_mergertree(halo, base_halo, must_include, request, visited=None, depth=0):
    if visited is None:
        visited = []
    start = time.time()
    recurse = halo.id not in visited
    visited.append(halo.id)

    rl = tangos.relation_finding.HopStrategy(halo, target=halo.timestep.previous)

    rl, weights = rl.all_and_weights()

    if len(rl) > 0:
        rl = [rli for rli, wi in zip(rl, weights) if wi > weights[0] * 0.02 or rli.id in must_include]

    timeinfo = "TS ...%s; z=%.2f; t=%.2e Gyr" % (
    halo.timestep.extension[-5:], halo.timestep.redshift, halo.timestep.time_gyr)

    if isinstance(halo, tangos.core.halo.BH):
        mass_name = "BH_mass"
        moreinfo = "BH %d" % halo.halo_number
    else:
        mass_name = "Mvir"

        if halo.NDM > 1e4:
            moreinfo = "%s %d, NDM=%.2e" % (halo.__class__.__name__, halo.halo_number, halo.NDM)
        else:
            moreinfo = "%s %d, NDM=%d" % (halo.__class__.__name__, halo.halo_number, halo.NDM)

    try:
        Mvir = halo.properties.filter_by(
            name_id=tangos.core.dictionary.get_dict_id(mass_name, session=tangos.core.Session.object_session(halo))).first()
    except KeyError:
        Mvir = None

    if Mvir is not None:
        moreinfo += ", %s=%.2e" % (mass_name, Mvir.data)
        unscaled_size = math.log10(Mvir.data)
    else:
        unscaled_size = math.log10(float(halo.NDM))
    nodeclass = 'node-dot-standard'

    name = str(halo.halo_number)
    start = time.time()
    if halo.links.filter_by(relation_id=tangos.core.dictionary.get_dict_id('Sub', -1)).count() > 0:
        nodeclass = 'node-dot-sub'
    #self.search_time += time.time() - start
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

    maxdepth = 0

    if recurse:
        for rli in rl:
            nx = _construct_preliminary_mergertree(rli, base_halo, must_include, request, visited, depth + 1)
            output['contents'].append(nx)
            if nx['maxdepth'] > maxdepth: maxdepth = nx['maxdepth']

    output['maxdepth'] = maxdepth + 1
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
            for i, child in enumerate(node['contents']):
                child_range = existing_ranges[child['depth']].get(child['halo_number'],
                                                                  (x_start + i * delta, x_start + (i + 1) * delta))

                child['space_range'] = child_range
                existing_ranges[child['depth']][child['halo_number']] = child_range

    _postprocess_mergertree_layout_by_number(tree, 'mid_range')


def _construct_mergertree(halo, request):
    search_time = 0
    start = time.time()
    base = halo
    must_include = []
    for i in range(5):
        must_include.append(base.id)
        if base.next is not None:
            base = base.next

    tree = _construct_preliminary_mergertree(base, halo, must_include, request)
    print("Merger tree build time:    %.2fs" % (time.time() - start))
    print("of which link search time: %.2fs" % (search_time))

    start = time.time()
    _postprocess_mergertree(tree)
    print("Post-processing time: %.2fs" % (time.time() - start))


    return tree

@view_config(route_name='merger_tree', renderer='json')
def merger_tree(request):
    halo = halo_from_request(request)

    return {'tree': _construct_mergertree(halo, request)}