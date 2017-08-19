from pyramid.view import view_config
from sqlalchemy import func, and_, or_
import numpy as np

import tangos
from tangos import core

def decode_property_name(name):
    name = name.replace("_slash_","/")
    return name

def format_data(data):
    data_fmt = []
    for d in data:
        if np.issubdtype(type(d), np.integer):
            data_fmt.append("%d"%d)
        elif np.issubdtype(type(d), np.float):
            if abs(d)>1e4:
                data_fmt.append("%.2e"%d)
            else:
                data_fmt.append("%.2f"%d)
        else:
            data_fmt.append(repr(d))
    return data_fmt

@view_config(route_name='gather_property', renderer='json')
def gather_property(request):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)

    try:
        halos, db_id = ts.gather_property(decode_property_name(request.matchdict['nameid']), 'dbid()')
    except Exception as e:
        return {'error': e.message, 'error_class': type(e).__name__}

    return {'timestep': ts.extension, 'data_formatted': format_data(halos),
           'db_id': list(db_id) }

@view_config(route_name='get_property', renderer='json')
def get_property(request):
    sim = tangos.get_simulation(request.matchdict['simid'], request.dbsession)
    ts = tangos.get_timestep(request.matchdict['timestepid'], request.dbsession, sim)
    halo = ts.halos.filter_by(halo_number=request.matchdict['halonumber']).first()

    result = halo.calculate(decode_property_name(request.matchdict['nameid']))
    return {'result': result}
