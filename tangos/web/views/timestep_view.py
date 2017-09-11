from __future__ import absolute_import
from pyramid.view import view_config
from sqlalchemy import func, and_, or_


import tangos
from tangos import core

@view_config(route_name='timestep_view', renderer='../templates/timestep_view.jinja2')
def timestep_view(request):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)

    halos = ts.halos.all()

    for h in halos:
        h.url = request.route_url('halo_view', simid=sim.basename, timestepid=ts.extension,
                                  halonumber=h.halo_number)

    return {'timestep': ts.extension, 'halos': halos,
            'gather_url': request.route_url('gather_property',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            nameid="")[:-5]}
