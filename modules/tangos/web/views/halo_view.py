from pyramid.view import view_config
import tangos
from tangos import core

class TimestepInfo(object):
    def __init__(self, ts):
        self.z = "%.2f"%ts.redshift
        self.t = "%.2e Gyr"%ts.time_gyr

class TimeLinks(object):
    def __init__(self, request, halo):
        link_names = ['earliest', '-10', '-1', '+1', '+10', 'latest']
        route_names = ['halo_earlier']*3 + ['halo_later']*3
        ns = ['ist',10,1,1,10,'ist']

        urls = [
            request.route_url(r, simid=halo.timestep.simulation.basename,
                              timestepid=halo.timestep.extension,
                              halonumber=halo.halo_number,
                              n=n)
            for r,n in zip(route_names, ns)
            ]

        self.urls = urls
        self.names = link_names

@view_config(route_name='halo_view', renderer='../templates/halo_view.jinja2')
def halo_view(request):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)
    halo = ts.halos.filter_by(halo_number=request.matchdict['halonumber']).first()

    return {'ts_info': TimestepInfo(ts),
            'this_id': halo.id,
            'halonumber': halo.halo_number,
            'timestep': ts.extension,
            'time_links': TimeLinks(request, halo)}