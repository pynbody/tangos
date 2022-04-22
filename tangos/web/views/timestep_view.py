from pyramid.view import view_config

from tangos import core

from . import timestep_from_request


@view_config(route_name='timestep_view', renderer='../templates/timestep_view.jinja2')
def timestep_view(request):
    ts = timestep_from_request(request)
    sim = ts.simulation

    all_objects = []

    typecode = 0
    while True:
        try:
            typetag = core.SimulationObjectBase.object_typetag_from_code(typecode)
        except ValueError:
            break

        n_objects = request.dbsession.query(core.SimulationObjectBase).\
            filter_by(timestep_id=ts.id, object_typecode=typecode).order_by(core.SimulationObjectBase.halo_number).count()

        title = core.SimulationObjectBase.class_from_tag(typetag).__name__+"s"

        if title=="BHs":
            title="Black holes"
        elif title=="PhantomHalos":
            title="Phantom halos"

        if n_objects>0:
            all_objects.append({'title': title, 'typetag': typetag, 'n_items': n_objects,
                                'object_url': request.route_url('halo_view',simid=sim.escaped_basename, timestepid=ts.escaped_extension,
                                                                halonumber=typetag+"_")
                                })

        typecode+=1


    return {'timestep': ts.extension,
            'objects': all_objects,
            'timestep_url': request.route_url('timestep_view',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'])}
