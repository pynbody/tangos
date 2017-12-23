from __future__ import absolute_import
from pyramid.view import view_config
from sqlalchemy import func, and_, or_
from . import timestep_from_request

import tangos
from tangos import core

def add_urls(halos, request, sim, ts):
    for h in halos:
        h.url = request.route_url('halo_view', simid=sim.basename, timestepid=ts.extension,
                                  halonumber=h.basename)

@view_config(route_name='timestep_view', renderer='../templates/timestep_view.jinja2')
def timestep_view(request):
    ts = timestep_from_request(request)
    sim = ts.simulation

    halos = ts.halos.all()
    groups = ts.groups.all()
    trackers = ts.trackers.all()

    add_urls(halos, request, sim, ts)
    add_urls(groups, request, sim, ts)
    add_urls(trackers, request, sim, ts)

    return {'timestep': ts.extension, 'halos': halos, 'groups': groups, 'trackers': trackers,
            'gather_url': request.route_url('calculate_all',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            nameid="")[:-5]}
