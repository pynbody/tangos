import logging

from pylons import request, response, session, tmpl_context as c, url
from pylons.controllers.util import abort, redirect

import halo_db.core.creator
from simdb.lib.base import BaseController, render

import simdb.lib.helpers as h

import simdb.model.meta as meta
from simdb.model.meta import *

import numpy as np

log = logging.getLogger(__name__)


class CreatorController(BaseController) :
    def index(self) :
        creators = Session.query(halo_db.core.creator.Creator).order_by(halo_db.core.creator.Creator.id.desc()).all()

        c.creators = []
        c.breadcrumbs = h.breadcrumbs(halo_db.core.creator.Creator)
        
        for x in creators :
            c.creators.append({"link": url(controller="creator", action="more", id=str(x.id)),
                               "username": x.username,
                               "host": x.host,
                               "command": x.command_line,
                               "time": x.dtime.strftime("%d/%m/%y %H:%M"),
                               "id": x.id})

        return render("/creator.mako")

    def more(self, id) :
        run = Session.query(halo_db.core.creator.Creator).filter_by(id=id).first()

        c.info=""
        c.info+="Was run by "+run.username+" on "+run.host+" as "+run.command_line+h.literal("<br/>")
        c.info+=run.dtime.strftime("%d/%m/%y %H:%M")+h.literal("<br/><br/>")
        
        if len(run.simulations)>0 :
            c.info+=str(len(run.simulations))+h.literal(" simulations<br/>")
        if (run.timesteps.count())>0 :
            c.info+=str(run.timesteps.count())+h.literal(" timesteps<br/>")
        if run.halos.count()>0:
            c.info+=str(run.halos.count())+h.literal(" halos<br/>")
        if run.halolinks.count()>0:
            c.info+=str(run.halolinks.count())+h.literal(" halolinks<br/>")
        if run.properties.count()>0:
            c.info+=str(run.properties.count())+h.literal(" halo properties<br/>")
        if run.simproperties.count()>0 :
            c.info+=str(run.simproperties.count())+h.literal(" simulation properties<br/>")

        c.delete=url(controller="creator", action="delete", id=str(run.id))
        c.breadcrumbs = h.breadcrumbs(run)

        return render("/creator_more.mako")

    def delete(self, id) :
        run = Session.query(halo_db.core.creator.Creator).filter_by(id=id).first()

        run.halolinks.delete()
        run.properties.delete()
        run.halos.delete()

        Session.delete(run)
        Session.commit()

        redirect(url(controller="creator", action="index"))
