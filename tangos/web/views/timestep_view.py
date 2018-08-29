from __future__ import absolute_import
from pyramid.view import view_config
from sqlalchemy import func, and_, or_
from . import timestep_from_request

import tangos
from tangos import core

def add_urls(halos, request, sim, ts):
    for h in halos:
        h.url = request.route_url('halo_view', simid=sim.escaped_basename, timestepid=ts.escaped_extension,
                                  halonumber=h.basename)

@view_config(route_name='timestep_view', renderer='../templates/timestep_view.jinja2')
def timestep_view(request):
    ts = timestep_from_request(request)
    sim = ts.simulation

    all_objects = []

    typecode = 0
    while True:
        try:
            typetag = core.Halo.object_typetag_from_code(typecode)
        except ValueError:
            break

        objects = request.dbsession.query(core.Halo).\
            filter_by(timestep_id=ts.id, object_typecode=typecode).order_by(core.Halo.halo_number).all()
        add_urls(objects, request, sim, ts)
        title = core.Halo.class_from_tag(typetag).__name__+"s"

        if title=="BHs":
            title="Black holes"
        elif title=="PhantomHalos":
            title="Phantom halos"

        all_objects.append({'title': title, 'typetag': typetag, 'items': objects})
        print(typecode, title, len(objects))
        typecode+=1


    return {'timestep': ts.extension,
            'objects': all_objects,
            'gather_url': request.route_url('calculate_all',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            nameid="")[:-5]}
