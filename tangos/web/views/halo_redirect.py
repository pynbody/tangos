from __future__ import absolute_import
from pyramid.view import view_config
import pyramid.httpexceptions as exc
import tangos
from . import halo_from_request

def halo_later_earlier(request, rel='later'):
    halo = halo_from_request(request)

    if request.matchdict['n']=='inf':
        if rel=='earlier':
            halo = halo.earliest
        if rel=='later':
            halo = halo.latest
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

    raise exc.HTTPFound(request.route_url("halo_view", simid=halo.timestep.simulation.escaped_basename,
                                          timestepid=halo.timestep.escaped_extension,
                                          halonumber=halo.basename))



@view_config(route_name='halo_later', renderer=None)
def halo_later(request):
    return halo_later_earlier(request, 'later')


@view_config(route_name='halo_earlier', renderer=None)
def halo_earlier(request):
    return halo_later_earlier(request, 'earlier')

@view_config(route_name='halo_in', renderer=None)
def halo_in(request):
    return halo_later_earlier(request, 'match')
