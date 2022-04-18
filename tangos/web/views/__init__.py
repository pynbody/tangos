from __future__ import absolute_import
import tangos
import pyramid.httpexceptions as exc

def halo_from_request(request):
    ts = timestep_from_request(request)
    try:
        halo = ts[request.matchdict['halonumber']]
    except (KeyError, ValueError):
        raise exc.HTTPNotFound()
    if halo is None:
        raise exc.HTTPNotFound()
    return halo

def timestep_from_request(request):
    sim = simulation_from_request(request)
    try:
        ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)
    except RuntimeError:
        raise exc.HTTPNotFound()
    return ts

def simulation_from_request(request):
    try:
        sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    except RuntimeError:
        raise exc.HTTPNotFound()
    return sim
