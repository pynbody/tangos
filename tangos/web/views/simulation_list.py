from __future__ import absolute_import
from pyramid.response import Response
from pyramid.view import view_config
import socket
import tangos
from tangos import core


@view_config(route_name='simulation_list', renderer='../templates/simulation_list.jinja2')
def simulation_list(request):
    session = request.dbsession

    cats = session.query(core.DictionaryItem).join(core.simulation.SimulationProperty).all()

    titles = ["Name"] + [z.text for z in cats]
    ids = [z.id for z in cats]

    sims = session.query(tangos.core.simulation.Simulation).all()
    simulations = []
    links = []

    for x in sims:
        s = [x.basename] + ["&ndash;"] * (len(titles) - 1)

        for q in x.properties:
            s[1 + ids.index(q.name_id)] = q.data_repr()

        simulations.append(s)
        links.append(request.route_url('simulation_view',simid=x.escaped_basename))


    return {'simulations':simulations, 'titles':titles, 'links':links,
            'hostname':socket.gethostname()}

