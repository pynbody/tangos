from __future__ import absolute_import
from pyramid.view import view_config
import pyramid.httpexceptions as exc
import tangos

def halo_later_earlier(request, rel='later'):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)
    halo = ts.halos.filter_by(halo_number=request.matchdict['halonumber']).first()
    if request.matchdict['n']=='inf':
        rel = rel[:-1]+"st" # later->latest, earlier->earliest
        try:
            halo = halo.calculate("%s()"%(rel))
        except tangos.live_calculation.NoResultsError:
            pass

    else:
        try:
            steps = int(request.matchdict['n'])

            if steps==0:
                return

            steps = str(steps)
        except ValueError:
            steps = "'%s'"%request.matchdict['n']

        try:
            halo = halo.calculate("%s(%s)"%(rel,steps))
        except tangos.live_calculation.NoResultsError:
            pass

    raise exc.HTTPFound(request.route_url("halo_view", simid=halo.timestep.simulation.basename,
                                          timestepid=halo.timestep.extension,
                                          halonumber=halo.halo_number))



@view_config(route_name='halo_later', renderer=None)
def halo_later(request):
    return halo_later_earlier(request, 'later')


@view_config(route_name='halo_earlier', renderer=None)
def halo_earlier(request):
    return halo_later_earlier(request, 'earlier')

@view_config(route_name='halo_in', renderer=None)
def halo_in(request):
    return halo_later_earlier(request, 'match')
