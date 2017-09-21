from __future__ import absolute_import
import tangos

def halo_from_request(request):
    ts = timestep_from_request(request)
    halo = ts[request.matchdict['halonumber']]
    return halo

def timestep_from_request(request):
    sim = simulation_from_request(request)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)
    return ts

def simulation_from_request(request):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    return sim
