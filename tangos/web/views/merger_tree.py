from pyramid.view import view_config

from ...config import webview_cache_time
from ...relation_finding import tree
from . import halo_from_request


class WebMergerTree(tree.MergerTree):
    def __init__(self, halo, request):
        self.request = request
        super().__init__(halo)

    def _get_basic_halo_node(self, halo, depth):
        output = super()._get_basic_halo_node(halo, depth)
        output['url'] = self.request.route_url('halo_view',
                                       simid=self.base_halo.timestep.simulation.escaped_basename,
                                       timestepid=halo.timestep.escaped_extension,
                                       halonumber=halo.basename)
        return output

def _construct_mergertree(halo, request):
    base = halo
    must_include = []
    for _ in range(5):
        must_include.append(base.id)
        if base.next is not None:
            base = base.next

    tree = WebMergerTree(base, request)
    tree.x_step = 30
    tree.must_include = must_include
    tree.highlight_halo = halo
    tree.construct()
    return tree._treedata


@view_config(route_name='merger_tree', renderer='json', http_cache=webview_cache_time)
def merger_tree(request):
    halo = halo_from_request(request)
    return {'tree': _construct_mergertree(halo, request)}
