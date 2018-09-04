from __future__ import absolute_import
from pyramid.view import view_config
from sqlalchemy import func

from . import simulation_from_request
import socket
import tangos
from tangos import core

@view_config(route_name='simulation_view', renderer='../templates/simulation_view.jinja2')
def simulation_view(request):
    session = request.dbsession

    sim = simulation_from_request(request)

    assert sim is not None

    timesteps = session.query(tangos.core.TimeStep).filter_by(simulation_id=sim.id).\
        order_by(tangos.core.timestep.TimeStep.time_gyr.desc()).all()
    counts = session.query(func.count(core.Halo.id)).\
        join(core.TimeStep).\
        filter(core.TimeStep.simulation_id==sim.id).\
        group_by(core.TimeStep.id). \
        order_by(tangos.core.timestep.TimeStep.time_gyr.desc()).\
        all()

    timestep_links = [request.route_url('timestep_view',simid=sim.escaped_basename,timestepid=timestep.escaped_extension)
                      for timestep in timesteps]

    counts = [c[0] for c in counts]

    simname = sim.basename

    props = []
    for q in sim.properties:
        props.append((q.name.text, q.data_repr()))

    return {'simulation':simname,
            'timesteps':timesteps,
            'links':timestep_links,
            'counts':counts,
            'properties':props
            }

