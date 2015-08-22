"""Helper functions

Consists of functions to typically be used within templates, but also
available to Controllers. This module is available to templates as 'h'.
"""
# Import helpers as desired, or define your own, ie:
#from webhelpers.html.tags import checkbox, password

from webhelpers.html import literal, HTML
from webhelpers.html.tags import *
import webhelpers.util as util
import simdb.model.meta as meta
from pylons import url

def oddeven(cycle=["odd","even"]) :
    while True :
	for x in cycle :
	    yield x

def breadcrumbs(from_object, with_link=False) :
    previous_item = None
    this = None

    if type(from_object) is meta.Simulation :
	if with_link :
	    this = link_to(from_object.basename, url(controller='sims', action='showsim', id=from_object.id))
	else :
	    this = from_object.basename

    elif type(from_object) is meta.TimeStep :
	this = from_object.extension
	if this[0]=="/" : this = this[1:]
	if with_link :
	    this = link_to(this, url(controller='sims', action='showstep', id=from_object.id))
	
	previous_item = from_object.simulation

    elif type(from_object) is meta.Halo :
	this = "halo_"+str(from_object.halo_number)
	if with_link :
	    this = link_to(this, url(controller='sims', action='showhalo', id=from_object.id))
	    
	previous_item = from_object.timestep
    elif type(from_object) is meta.HaloProperty :

	this = str(from_object.name.text)

	if with_link :
	    this = link_to(this, url(controller='plot', action='time', id=from_object.id))

	previous_item = from_object.halo
    elif type(from_object) is meta.Creator :
        if with_link :
            this = link_to(str(from_object.id), url(controller="creator", action="more", id=from_object.id))
        else :
            this = str(from_object.id)
        previous_item = meta.Creator
        
    elif from_object is meta.Creator :
        if with_link :
            this = link_to("run", url(controller="creator", action="index"))
        else :
            this = "run"
    else :
	this = "simdb"
	if with_link :
	    this = link_to(this, url(controller='sims', action='index'))
	    
	return this

    return breadcrumbs(previous_item,with_link=True)+"/"+this
