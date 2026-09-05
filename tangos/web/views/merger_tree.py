from pyramid.view import view_config

from ... import core
from ...config import mergertree_timeout, webview_cache_time
from ...relation_finding import tree
from . import halo_from_request


class WebMergerTreeLayout(tree.MergerTreeLayout):
    """A merger tree layout carrying the labels and links needed by the web interface"""

    def __init__(self, merger_tree, request, highlight=None, with_calculations=None,
                 x_step=30):
        """Set up a layout for display in the web interface.

        :param merger_tree: the tree to lay out
        :param request: the pyramid request, used to generate links to each object
        :param highlight: the object to mark as selected, if any
        :param with_calculations: names of properties to display alongside each object;
          defaults to ["Mvir"] if Mvir is in the database, and to no properties otherwise
        """
        super().__init__(merger_tree, x_step=x_step)

        self.request = request
        self.highlight = highlight

        if with_calculations is None:
            if core.get_dict_id("Mvir", -1) != -1:
                with_calculations = ["Mvir"]
            else:
                with_calculations = []
        self.with_calculations = with_calculations

        self._property_values = dict(zip(
            with_calculations, merger_tree.calculate_all(*with_calculations)))

    def node_dict(self, node):
        output = super().node_dict(node)
        obj = node.obj

        timeinfo = "TS ...{}; z={:.2f}; t={:.2e} Gyr".format(
            obj.timestep.extension[-5:], obj.timestep.redshift, obj.timestep.time_gyr)

        if obj.NDM > 0:
            moreinfo = "%s %d, NDM=%.2e" % (obj.__class__.__name__, obj.halo_number, obj.NDM)
        else:
            moreinfo = "%s %d" % (obj.__class__.__name__, obj.halo_number)

        Mvir = self._property_values.get("Mvir", None)
        if Mvir is not None and Mvir[node.index] == Mvir[node.index]: # i.e. not NaN
            moreinfo += ", {}={:.2e}".format("Mvir", Mvir[node.index])

        nodeclass = 'node-dot-standard'
        name = str(obj.halo_number)

        if obj == self.highlight:
            nodeclass = 'node-dot-selected'

        if isinstance(obj, core.halo.PhantomHalo):
            nodeclass += ' phantom'

        if node.depth == 0:
            if obj.next is not None:
                nodeclass = 'node-dot-continuation'
                name = '...'
                moreinfo = "Continues... " + moreinfo

        if len(name) > 4:
            name = ""

        output.update({'name': name,
                       'nodeclass': nodeclass,
                       'moreinfo': moreinfo,
                       'timeinfo': timeinfo,
                       'url': self.request.route_url(
                           'halo_view',
                           simid=self.tree.base_object.timestep.simulation.escaped_basename,
                           timestepid=obj.timestep.escaped_extension,
                           halonumber=obj.basename)})
        return output


def _construct_mergertree(halo, request):
    base = halo
    must_include = []
    for i in range(5):
        must_include.append(base.id)
        if base.next is not None:
            base = base.next

    merger_tree = tree.MergerTree(base, must_include=must_include,
                                  timeout=mergertree_timeout)
    return WebMergerTreeLayout(merger_tree, request, highlight=halo).as_dict()


@view_config(route_name='merger_tree', renderer='json', http_cache=webview_cache_time)
def merger_tree(request):
    halo = halo_from_request(request)
    return {'tree': _construct_mergertree(halo, request)}
