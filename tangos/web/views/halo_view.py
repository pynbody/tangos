from __future__ import absolute_import
from pyramid.view import view_config
import tangos
from tangos import core
import numpy as np
from .halo_data import format_number, _relative_description
import sqlalchemy, sqlalchemy.orm
from six.moves import zip
from . import halo_from_request

class TimestepInfo(object):
    def __init__(self, ts):
        self.z = "%.2f"%ts.redshift
        self.t = "%.2e Gyr"%ts.time_gyr


class TimeLinks(object):
    def __init__(self, request, halo):
        link_names = ['earliest', '-10', '-1', '+1', '+10', 'latest']
        route_names = ['halo_earlier']*3 + ['halo_later']*3
        ns = ['inf',10,1,1,10,'inf']

        urls = [
            request.route_url(r, simid=halo.timestep.simulation.escaped_basename,
                              timestepid=halo.timestep.escaped_extension,
                              halonumber=halo.basename,
                              n=n)
            for r,n in zip(route_names, ns)
            ]

        self.urls = urls
        self.names = link_names

class DisplayProperty(object):
    def __init__(self, property):
        self.name = property.name.text
        self.value = format_property_data(property)
        self.is_array = property.data_is_array()

class TimeProperty(DisplayProperty):
    def __init__(self, halo):
        self.name = "t()"
        self.value = format_number(halo.timestep.time_gyr) + " Gyr"
        self.is_array = False

class RedshiftProperty(DisplayProperty):
    def __init__(self, halo):
        self.name = "z()"
        self.value = format_number(halo.timestep.redshift)
        self.is_array = False

def default_properties(halo):
    properties = [TimeProperty(halo), RedshiftProperty(halo)]

    for property in halo.properties.options(sqlalchemy.orm.joinedload(core.HaloProperty.name)):
        properties.append(DisplayProperty(property))

    return properties

def format_property_data(property):
    if property.data_is_array():
        """
        data = property.data_raw
        if len(data)>5 or len(data.shape)>1:
            return "size "+(" x ".join([str(s) for s in data.shape]))+" array"
        else:
            return "["+(",".join([_number_format(d) for d in data]))+"]"
        """
        return "Array"
    else:
        return format_number(property.data)

class SimulationInfo(object):
    def __init__(self, sim, request):
        self.name = sim.basename
        self.url = request.route_url('halo_in',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            halonumber=request.matchdict['halonumber'],
                                            n=sim.basename)


class HaloLinkInfo(object):
    def __init__(self, link, request):
        halo_source = link.halo_from
        halo_dest = link.halo_to
        weight_text = "( %.2f)"%link.weight if link.weight else ""
        self.name = "%s%s: %s"%(link.relation.text,weight_text,_relative_description(halo_source, halo_dest))
        self.url = request.route_url('halo_view', simid=halo_dest.timestep.simulation.escaped_basename,
                                     timestepid=halo_dest.timestep.escaped_extension,
                                     halonumber=halo_dest.basename)

def all_simulations(request):
    return [SimulationInfo(x,request) for x in tangos.all_simulations(request.dbsession)]

def halo_links(halo, request):
    links = []
    links_query = request.dbsession.query(core.HaloLink).filter_by(halo_from_id=halo.id).\
            order_by(core.HaloLink.weight.desc()).\
            options(sqlalchemy.orm.joinedload(core.HaloLink.halo_to).joinedload(core.Halo.timestep).joinedload(core.TimeStep.simulation))

    for lk in links_query.all():
        links.append(HaloLinkInfo(lk, request))
    return links

@view_config(route_name='halo_view', renderer='../templates/halo_view.jinja2')
def halo_view(request):
    halo = halo_from_request(request)
    ts = halo.timestep
    sim = ts.simulation

    return {'ts_info': TimestepInfo(ts),
            'this_id': halo.id,
            'halo_number': halo.halo_number,
            'halo_typetag': halo.tag,
            'timestep': ts.extension,
            'simulation': sim.basename,
            'all_simulations': all_simulations(request),
            'halo_links': halo_links(halo, request),
            'time_links': TimeLinks(request, halo),
            'properties': default_properties(halo),
            'halo_path': halo.path,
            'finder_id': halo.finder_id,
            'calculate_url': request.route_url('get_property',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            halonumber=request.matchdict['halonumber'],
                                            nameid="")[:-5],
            'tree_url': request.route_url('merger_tree',simid=request.matchdict['simid'],
                                            timestepid=request.matchdict['timestepid'],
                                            halonumber=request.matchdict['halonumber']),
            'gather_url': "/%s/%s/"%(sim.escaped_basename,ts.escaped_extension),
            'cascade_url': "/%s/%s/%s/"%(sim.escaped_basename,ts.escaped_extension,halo.basename)}