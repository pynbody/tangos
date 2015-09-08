import logging

from pylons import request, response, session, tmpl_context as c, url
from pylons.controllers.util import abort, redirect

from simdb.lib.base import BaseController, render

import simdb.lib.helpers as h

import simdb.model.meta as meta
from simdb.model.meta import *

import halo_db as db

import numpy as np

log = logging.getLogger(__name__)

def creator_link(creator) :
    return h.link_to(repr(creator), url(controller="creator", action="more", id=creator.id))

class SimsController(BaseController):

    def index(self):

        cats = Session.query(meta.DictionaryItem).join(meta.SimulationProperty).all()

        c.titles = ["Name"]+[z.text for z in cats]
        ids = [z.id for z in cats]



        sims = Session.query(meta.Simulation).all()



        c.sims = []


        for x in sims :
            s = [h.link_to(x.basename, url(controller="sims", action="showsim", id=str(x.id)))
                 ]+[h.literal("&ndash;")]*(len(c.titles)-1)

            for q in x.properties :
                s[1+ids.index(q.name_id)] = q.data_repr()

            c.sims.append({"link":url(controller="sims", action="showsim", id=str(x.id)),
                           "table":s})

        c.breadcrumbs = h.breadcrumbs(None)

        return render('/sims.mako')





    def showsim(self, id) :
        sim = Session.query(meta.Simulation).filter_by(id=id).first()
        c.timesteps = Session.query(meta.TimeStep).filter_by(simulation_id=id).order_by(meta.TimeStep.time_gyr.desc()).all()
        c.basename = sim.basename
        c.breadcrumbs = h.breadcrumbs(sim)
        c.creator = creator_link(sim.creator)
        c.props = []
        for q in sim.properties :
            c.props.append((q.name.text, q.data_repr()))

        return render('/sims/showsim.mako')

    def _apply_filters(self, query) :
        if session.get('nocontam',False) :
            query = query.join(meta.HaloProperty).filter(
                meta.HaloProperty.name_id==db.get_dict_id('contamination_fraction',-1,session=query.session),
                meta.HaloProperty.data_float<1e-5)
        if session.get('nosub',False) :
            query = query.filter(~meta.Halo.links.any(db.HaloLink.relation_id==db.get_dict_id('Sub',-1,session=query.session)))
        return query

    def showstep(self, id) :
        if 'update' in request.GET :
            session['nosub'] = 'nosub' in request.GET
            session['nocontam'] = 'nocontam' in request.GET
            session.save()

        step = Session.query(meta.TimeStep).filter_by(id=id).first()
        c.halos = self._apply_filters(step.halos.filter(meta.and_(meta.Halo.halo_type==0,
                                              meta.or_(meta.Halo.NDM>5000,meta.Halo.NDM==0)))).all()

        c.tinyhalos = self._apply_filters(step.halos.filter(meta.and_(meta.Halo.halo_type==0,meta.Halo.NDM<=5000,
                                              meta.or_(meta.Halo.NDM>100,meta.Halo.NDM==0)))).all()

        c.trackerhalos = step.halos.filter(meta.Halo.halo_type==1).all()
        c.parent_id = step.simulation.id
        c.name = str(step.simulation.basename)+"/"+str(step.extension)
        c.breadcrumbs = h.breadcrumbs(step)
        c.creator = creator_link(step.creator)

        return render('/sims/showstep.mako')

    def _relative_description(self, this_halo, other_halo) :
        if other_halo is None :
            return "null"
        if this_halo.timestep_id == other_halo.timestep_id :
            return "halo %d"%(other_halo.halo_number)
        elif this_halo.timestep.simulation_id == other_halo.timestep.simulation_id :
            return "halo %d at t=%.2e Gyr"%(other_halo.halo_number, other_halo.timestep.time_gyr)
        else :
            return "halo %d in %8s at t=%.2e Gyr"%(other_halo.halo_number, other_halo.timestep.simulation.basename, other_halo.timestep.time_gyr)

    def _relative_link(self, this_halo, other_halo) :
        if other_halo is None or other_halo==this_halo :
            return self._relative_description(this_halo,other_halo)
        else :
            return h.link_to(self._relative_description(this_halo, other_halo),
                             url(controller="sims", action="showhalo", id=other_halo.id))

    def _render_bh(self, links, reverse_links) :
        rt = ""
        present = False

        for hl in reverse_links:
            if hl.relation.text.startswith("BH"):
                rt = h.literal("<h2>You are looking at a black hole</h2><p>")
                rt +=h.link_to("< Back to safety",
                                 url(controller="sims", action="showhalo", id=hl.halo_from.id))
                rt += h.literal("</p>")
        for hl in links:
            if hl.relation.text.startswith("BH"):
                present = True

        if present:
            rt = h.literal("<h2>Black holes</h2><p>")
            separate = False
            for hl in links:
                if hl.relation.text.startswith("BH"):
                    if separate: rt+=" | "
                    separate = True
                    rt+=h.link_to(hl.relation.text,
                                     url(controller="sims", action="showhalo", id=hl.halo_to.id))
            rt+=h.literal("</p>")
        return rt

    def _render_links(self, links, reverse_links) :
        rt =""
        rt = h.literal("<table><tr><th>Source</th><th>Relationship</th><th>Target</th>")
        for hl in links :
            rt+=h.literal("<tr><td>this</td>")
            rt+=h.literal("<td>"+hl.relation.text+"</td>")
            rt+=h.literal("<td>")+self._relative_link(hl.halo_from,hl.halo_to)+h.literal("</td></tr>")
        for hl in reverse_links :
            rt+=h.literal("<tr><td>")+self._relative_link(hl.halo_to,hl.halo_from)+h.literal("</td>")
            rt+=h.literal("<td>"+hl.relation.text+"</td>")
            rt+=h.literal("<td>this</td></tr>")
        rt+=h.literal("</table>")
        return rt

    def _render_properties(self, properties, links=True, parents=False) :
        rt = ""

        for k in properties :
            if ":" in k.name.text :
                continue
            rt+=h.literal("<tr>")

            if k.data_is_array() :
                if links :

                    rt+=h.HTML.td(h.HTML.input(type="radio",name="x",value=k.name.text,
                                               id="RIx_"+k.name.text,
                                               class_="radio_array"))
                    rt+=h.HTML.td(h.HTML.input(type="radio",name="y",value=k.name.text,
                                               id="RIy_"+k.name.text,
                                               class_="radio_array"))



                    rt+=h.HTML.td(h.link_to("Plot", url(controller='plot', action='image_img', id=k.id),
                                            onclick = "gograph('"+url(controller='plot', action='image_img', id=k.id)+"'); return false; "), h.literal(" / "), \

                                  h.link_to("Get >",url(controller='plot', action='array_img', id=k.id, text=True)))



                rt+=h.HTML.td(k.name.text)

            else :
                if links :
                    rt+=h.HTML.td(h.HTML.input(type="radio",name="x",value=k.name.text,class_="radio_scalar"))
                    if k.name.text=="Mvir" :
                        rt+=h.HTML.td(h.HTML.input(type="radio",name="y",value=k.name.text,checked=True,class_="radio_scalar"))
                    else :
                        rt+=h.HTML.td(h.HTML.input(type="radio",name="y",value=k.name.text,class_="radio_scalar"))
                    rt+=h.HTML.td("")

                if isinstance(k.data, int) :
                    rt+=h.HTML.td("%s = %d"%(k.name.text, k.data))
                elif isinstance(k.data, float) :
                    rt+=h.HTML.td("%s = %.2e"%(k.name.text, k.data))
                else :
                    rt+=h.HTML.td("%s = %s"%(k.name.text, repr(k.data)))
            if parents :
                rt+=" "+h.HTML.td(self.creator_link_cache(k.creator))
            else :
                rt+=h.HTML.td("")

            rt+=h.literal("<tr/>")


        return rt

    creator_cache = {}
    def creator_link_cache(self, creator) :
        if creator.id not in self.creator_cache :
            self.creator_cache[creator.id]=len(self.creator_cache)+1
        cid = str(self.creator_cache[creator.id])
        return h.link_to(cid,url(controller="creator", action="more", id=creator.id))

    def creator_link_cache_table(self) :
        tx = ""
        for k, v in sorted(creator_cache.iteritems()) :
            tx+="["+str(v)+"] "+self.creator_link(k)+"<br/>"
        self.creator_cache = {}
        return tx

    def _constr_mergertree(self, halo, base_halo):
        rl = halo.reverse_links.filter_by(relation_id=db.get_dict_id('time')).all()

        timeinfo = "TS ...%s; z=%.2f; t=%.2e Gyr"%(halo.timestep.extension[-5:], halo.timestep.redshift, halo.timestep.time_gyr)

        if halo.NDM>1e4:
            moreinfo = "Halo %d, NDM=%.2e"%(halo.halo_number, halo.NDM)
        else:
            moreinfo = "Halo %d, NDM=%d"%(halo.halo_number, halo.NDM)

        Mvir = halo.properties.filter_by(name_id=db.get_dict_id('Mvir')).first()
        if Mvir is not None:
            moreinfo+=", Mvir=%.2e"%Mvir.data
        nodeclass = 'node-dot-standard'

        if halo.links.filter_by(relation_id=db.get_dict_id('Sub')).count()>0:
            nodeclass = 'node-dot-sub'

        output = {'name': str(halo.halo_number),
                  'url': url(controller="sims", action="showhalo", id=halo.id),
                  'nodeclass': nodeclass,
                  'moreinfo': moreinfo,
                  'timeinfo': timeinfo,
                  '_x': halo.halo_number*10,
                  'contents': [] }

        maxdepth = 0

        for rli in rl:
            nx = self._constr_mergertree(rli.halo_from, base_halo)
            output['contents'].append(nx)
            if nx['maxdepth']>maxdepth: maxdepth = nx['maxdepth']

        output['maxdepth'] = maxdepth+1
        return output


    def mergertree(self, id):
        layers = []
        halo = Session.query(meta.Halo).filter_by(id=id).first()
        c.tree = self._constr_mergertree(halo,halo)
        c.breadcrumbs = h.breadcrumbs(halo)
        return render('/sims/mergertree.mako')


    def showhalo(self, id, rel=None,num=1) :

        halo = Session.query(meta.Halo).filter_by(id=id).first()
        c.flash = []

        if rel is not None :
            num = int(num)
            if rel=="earlier" :
                nhalo = halo
                for i in xrange(num) :
                    if nhalo.previous is not None:
                        nhalo = nhalo.previous
                if nhalo is not None and nhalo.id==halo.id:
                    c.flash = ["You're already looking at the earliest snapshot for this halo"]
            elif rel=="earliest" :
                nhalo = halo.earliest
                if nhalo is not None and nhalo.id==halo.id:
                    c.flash = ["You're already looking at the earliest snapshot for this halo"]
            elif rel=="later" :
                nhalo = halo
                for i in xrange(num) :
                    if nhalo.next is not None:
                        nhalo = nhalo.next
                if nhalo is not None and nhalo.id==halo.id:
                    c.flash = ["You're already looking at the latest snapshot for this halo"]
            elif rel=="latest" :
                nhalo = halo.latest
                if nhalo is not None and nhalo.id==halo.id:
                    c.flash = ["You're already looking at the latest snapshot for this halo"]
            elif rel=="insim":
                nhalo = halo.get_linked_halos_from_target(Session.query(meta.Simulation).filter_by(id=num).first())
                print nhalo
                if len(nhalo)==0:
                    nhalo = None
                else:
                    nhalo = nhalo[0]

                if nhalo is not None and nhalo.id==halo.id:
                    c.flash = ["You're already looking at this simulation"]
            else :
                c.flash = ["I have never heard of the relationship %r, so can't find the halo for you"%rel]

            if nhalo is None and c.flash==[]:
                c.flash = ["Can't find the halo you are looking for"]

            if len(c.flash)==0:
                redirect(url(controller='sims',action='showhalo',id=nhalo.id))
                return


        c.name = "Halo "+str(halo.halo_number)+" of "+halo.timestep.extension

        c.props = self._render_properties(halo.properties, parents=True)
        c.dep_props = self._render_properties(halo.deprecated_properties, links=False, parents=True)
        c.bh = self._render_bh(halo.links, halo.reverse_links)
        c.links = self._render_links(halo.links, halo.reverse_links)

        all_sims = db.all_simulations(Session)

        c.sims = [(sim.basename, sim.id, sim.id==halo.timestep.simulation_id) for sim in all_sims]



        c.ndm = halo.NDM
        c.nbar = halo.NGas + halo.NStar

        c.ndarray = np.ndarray
        c.timestep_id = halo.timestep.id
        c.simulation_id = halo.timestep.simulation_id
        c.timestep_z = halo.timestep.redshift
        if halo.timestep.time_gyr<1 :
            c.timestep_t = "%.0f Myr"%(1000*halo.timestep.time_gyr)
        else :
            c.timestep_t = "%.2f Gyr"%halo.timestep.time_gyr
        c.this_id = id


        c.breadcrumbs = h.breadcrumbs(halo)



        return render('/sims/showhalo.mako')
