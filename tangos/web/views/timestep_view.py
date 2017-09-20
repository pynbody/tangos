from __future__ import absolute_import
from pyramid.view import view_config
from sqlalchemy import func, and_, or_


import tangos
from tangos import core

def add_urls(halos, request, sim, ts):
    for h in halos:
        h.url = request.route_url('halo_view', simid=sim.basename, timestepid=ts.extension,
                                  halonumber=h.basename)

@view_config(route_name='timestep_view', renderer='../templates/timestep_view.jinja2')
def timestep_view(request):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)

    halos = ts.halos.all()
    groups = ts.groups.all()

    add_urls(halos, request, sim, ts)
    add_urls(groups, request, sim, ts)


    for h in groups:
        h.url = request.route_url('halo_view', simid=sim.basename, timestepid=ts.extension,
                                  halonumber=h.basename)

    return {'timestep': ts.extension, 'halos': halos, 'groups': groups,
            'gather_url': request.route_url('gather_property',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            nameid="")[:-5]}
