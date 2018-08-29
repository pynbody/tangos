from __future__ import absolute_import
from __future__ import print_function
import matplotlib
matplotlib.use('agg')
import pylab as p
from pyramid.view import view_config
from pyramid.compat import escape
import numpy as np
from . import halo_from_request, timestep_from_request, simulation_from_request
from pyramid.response import Response
from six import BytesIO, string_types
from ...log import logger
from ... import core
import threading
import time

_matplotlib_lock = threading.RLock()

def decode_property_name(name):
    name = name.replace("_slash_","/")
    return name

def format_array(data, max_array_length=3):
    if len(data)>max_array_length:
        return "Array"
    data_fmt = []
    for d in data:
        data_fmt.append(format_data(d))
    return "["+(", ".join(data_fmt))+"]"

def format_number(data):
    if np.issubdtype(type(data), np.integer):
        return "%d" % data
    elif np.issubdtype(type(data), np.float):
        if abs(data) > 1e5 or abs(data) < 1e-2:
            return "%.2e" % data
        else:
            return "%.2f" % data

def format_data(data, request=None, relative_to=None, max_array_length=3):
    if hasattr(data,'__len__'):
        return format_array(data, max_array_length)
    elif np.issubdtype(type(data), np.number):
        return format_number(data)
    elif isinstance(data, core.Halo):
        return format_halo(data, request, relative_to)
    else:
        return escape(repr(data))



def _relative_description(this_halo, other_halo) :
    if other_halo is None :
        return "null"
    elif this_halo and this_halo.id==other_halo.id:
        return "this"
    elif this_halo and this_halo.timestep_id == other_halo.timestep_id :
        return "%s %d"%(other_halo.tag,other_halo.halo_number)
    elif this_halo and this_halo.timestep.simulation_id == other_halo.timestep.simulation_id :
        return "%s %d at t=%.2e Gyr"%(other_halo.tag,other_halo.halo_number, other_halo.timestep.time_gyr)
    else :
        if (not this_halo) or abs(this_halo.timestep.time_gyr - other_halo.timestep.time_gyr)>0.001:
            return "%s %d in %8s at t=%.2e Gyr"%(other_halo.tag,other_halo.halo_number, other_halo.timestep.simulation.basename,
                                                   other_halo.timestep.time_gyr)
        else:
            return "%s %d in %8s"%(other_halo.tag,other_halo.halo_number, other_halo.timestep.simulation.basename)


def format_halo(halo, request, relative_to=None):
    if relative_to==halo or request is None:
        return _relative_description(relative_to, halo)
    else:
        link = request.route_url('halo_view', simid=halo.timestep.simulation.escaped_basename,
                                 timestepid=halo.timestep.escaped_extension,
                                 halonumber=halo.basename)
        return "<a href='%s'>%s</a>"%(link, _relative_description(relative_to, halo))

def can_use_in_plot(data):
    return np.issubdtype(type(data), np.number)

def can_use_elements_in_plot(data_array):
    if len(data_array)==0:
        return False
    else:
        return can_use_in_plot(data_array[0])

def can_use_as_filter(data):
    return np.issubdtype(type(data), np.bool_) and not np.issubdtype(type(data), np.number) and not hasattr(data,'__len__')

def can_use_elements_as_filter(data_array):
    if len(data_array)==0:
        return False
    else:
        return can_use_as_filter(data_array[0])

def is_array(data):
    return isinstance(data, np.ndarray) and data.ndim>0

def elements_are_arrays(data_array):
    if len(data_array)==0:
        return False
    else:
        return is_array(data_array[0])

@view_config(route_name='calculate_all', renderer='json')
def calculate_all(request):
    ts = timestep_from_request(request)

    try:
        data, db_id = ts.calculate_all(decode_property_name(request.matchdict['nameid']), 'dbid()')
    except Exception as e:
        return {'error': getattr(e,'message',""), 'error_class': type(e).__name__}

    return {'timestep': ts.escaped_extension, 'data_formatted': [format_data(d, request) for d in data],
           'db_id': [int(x) for x in db_id],  # problems with jsonifying np.int64; needs to be native int?
            'can_use_in_plot': can_use_elements_in_plot(data),
            'can_use_as_filter': can_use_elements_as_filter(data),
            'is_array': elements_are_arrays(data)}

@view_config(route_name='get_property', renderer='json')
def get_property(request):
    halo = halo_from_request(request)

    try:
        result = halo.calculate(decode_property_name(request.matchdict['nameid']))
    except Exception as e:
        return {'error': getattr(e,'message',""), 'error_class': type(e).__name__}

    return {'data_formatted': format_data(result, request, halo),
            'can_use_in_plot': can_use_in_plot(result),
            'can_use_as_filter': can_use_as_filter(result),
            'is_array': is_array(result)}


def start(request) :
    p.ioff()
    p.clf()
    request.canvas =  p.get_current_fig_manager().canvas

def finish(request, getImage=True) :
    if getImage:
        enter_finish_time = time.time()
        request.canvas.draw()
        draw_time = time.time()
        buffer = BytesIO()
        p.savefig(buffer, format='png')
        end_time = time.time()
        logger.info("Image rendering: matplotlib %.2fs; PNG conversion %.3fs",draw_time-enter_finish_time, end_time-draw_time)

    p.close()

    if getImage :
        return Response(content_type='image/png',body=buffer.getvalue())


def rescale_plot(request):
    logx = request.GET.get('logx',False)
    logy = request.GET.get('logy',False)
    if logx and logy:
        p.loglog()
    elif logx:
        p.semilogx()
    elif logy:
        p.semilogy()

@view_config(route_name='gathered_plot')
def gathered_plot(request):
    start_time = time.time()
    name1, name2, v1, v2 = gathered_plot_data(request)
    logger.info("Gathering data took %.2fs"%(time.time()-start_time))
    with _matplotlib_lock:
        start(request)
        p.plot(v1,v2,'k.')
        p.xlabel(name1)
        p.ylabel(name2)
        rescale_plot(request)
        return finish(request)


@view_config(route_name='gathered_csv', renderer='csv')
def gathered_csv(request):
    name1, name2, v1, v2 = gathered_plot_data(request)
    return {
        'header': [name1, name2],
        'rows': np.array((v1,v2)).T,
        'name': "timestep_" + name1 + "_vs_" + name2
    }

def gathered_plot_data(request):
    ts = timestep_from_request(request)
    name1 = decode_property_name(request.matchdict['nameid1'])
    name2 = decode_property_name(request.matchdict['nameid2'])
    filter = decode_property_name(request.GET.get('filter', ""))
    object_typetag = request.GET.get('object_typetag', None)

    if filter != "":
        v1, v2, f = ts.calculate_all(name1, name2, filter, object_typetag=object_typetag)
        v1 = v1[f]
        v2 = v2[f]
    else:
        v1, v2 = ts.calculate_all(name1, name2, object_typetag=object_typetag)

    return name1, name2, v1, v2



@view_config(route_name='cascade_plot')
def cascade_plot(request):
    name1, name2, v1, v2 = cascade_plot_data(request)
    with _matplotlib_lock:
        start(request)
        p.plot(v1,v2,'k')
        p.xlabel(name1)
        p.ylabel(name2)
        rescale_plot(request)
        return finish(request)

@view_config(route_name='cascade_csv', renderer='csv')
def cascade_csv(request):
    name1, name2, v1, v2 = cascade_plot_data(request)
    return {
        'header': [name1, name2],
        'rows': np.array((v1,v2)).T,
        'name': "timeseries_"+name1+"_vs_"+name2
    }

def cascade_plot_data(request):
    halo = halo_from_request(request)
    name1 = decode_property_name(request.matchdict['nameid1'])
    name2 = decode_property_name(request.matchdict['nameid2'])
    v1, v2 = halo.calculate_for_progenitors(name1, name2)
    return name1, name2, v1, v2


def image_plot(request, val, property_info):

    log=request.GET.get('logimage',False)
    with _matplotlib_lock:
        start(request)

        if property_info:
            width = property_info.plot_extent()
        else:
            width = 1.0

        if log and len(val.shape)==2:
            data = np.log10(val)
            data[data!=data]=data[data==data].min()

        else:
            data =val

        if width is not None :
            p.imshow(data,extent=(-width/2,width/2,-width/2,width/2))
        else :
            p.imshow(data)

        if property_info:
            add_xy_labels(property_info, request)

        if len(val.shape) is 2 :
            cb = p.colorbar()
            if property_info and property_info.plot_clabel() :
                cb.set_label(property_info.plot_clabel())

        return finish(request)


def add_xy_labels(property_info, request):
    p.xlabel(property_info.plot_xlabel())
    ylabel = property_info.plot_ylabel()
    # cludge follows - should be eliminated by fixing the mess around multi-name vs single-name property classes
    if not isinstance(ylabel, string_types):
        try:
            ylabel = ylabel[property_info.index_of_name(decode_property_name(request.matchdict['nameid']))]
        except:
            ylabel = ""
    p.ylabel(ylabel)


@view_config(route_name='array_plot')
def array_plot(request):
    halo = halo_from_request(request)
    name = decode_property_name(request.matchdict['nameid'])


    val, property_info = halo.calculate(name, True)

    if len(val.shape)>1:
        return image_plot(request, val, property_info)

    with _matplotlib_lock:
        start(request)

        p.plot(property_info.plot_x_values(val),val)

        if property_info.plot_xlog() and property_info.plot_ylog():
            p.loglog()
        elif property_info.plot_xlog():
            p.semilogx()
        elif property_info.plot_ylog():
            p.semilogy()


        if property_info.plot_yrange():
            p.ylim(*property_info.plot_yrange())

        add_xy_labels(property_info, request)

        return finish(request)


@view_config(route_name='array_csv', renderer='csv')
def array_csv(request):
    halo = halo_from_request(request)
    name = decode_property_name(request.matchdict['nameid'])
    val, property_info = halo.calculate(name, True)
    xval = property_info.plot_x_values(val)

    return {
        'header': ["bin_center", name],
        'rows': np.array((xval,val)).T,
    }