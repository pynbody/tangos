import logging
import time

from pylons import request, response, session, tmpl_context as c, url
from pylons.controllers.util import abort, redirect

from simdb.lib.base import BaseController, render

import simdb.lib.helpers as h

import simdb.model.meta as meta
from simdb.model.meta import *

import halo_db as db
import halo_db.hopper
import math
import numpy as np

log = logging.getLogger(__name__)

MAXHOPS_FIND_HALO = 3

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
        elif this_halo.id==other_halo.id:
            return "this"
        elif this_halo.timestep_id == other_halo.timestep_id :
            return "halo %d"%(other_halo.halo_number)
        elif this_halo.timestep.simulation_id == other_halo.timestep.simulation_id :
            return "halo %d at t=%.2e Gyr"%(other_halo.halo_number, other_halo.timestep.time_gyr)
        else :
            if abs(this_halo.timestep.time_gyr - other_halo.timestep.time_gyr)>0.001:
                return "halo %d in %8s at t=%.2e Gyr"%(other_halo.halo_number, other_halo.timestep.simulation.basename,
                                                       other_halo.timestep.time_gyr)
            else:
                return "halo %d in %8s"%(other_halo.halo_number, other_halo.timestep.simulation.basename)

    def _relative_link(self, this_halo, other_halo) :
        if other_halo is None or other_halo==this_halo :
            return self._relative_description(this_halo,other_halo)
        else :
            linkurl = url(controller="sims", action="showhalo", id=other_halo.id)
            return h.link_to(self._relative_description(this_halo, other_halo),
                             linkurl,
                             onclick="return timeNav('"+linkurl+"');")

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
        rt = h.literal("<table><tr><th>Source</th><th>Relationship</th><th>Target</th><th>Weight</th></tr>")
        for hl in links :
            rt += self._render_link(hl.halo_from,hl)
        for hl in reverse_links :
            rt += self._render_link(hl.halo_to,hl)
        rt+=h.literal("</table>")
        return rt

    def _render_link(self, relative_to, link):
        text = h.literal("<tr><td>") + self._relative_link(relative_to, link.halo_from)+ h.literal("</td>")
        text += h.literal("<td>" + link.relation.text + "</td>")
        text += h.literal("<td>") + self._relative_link(relative_to, link.halo_to) + h.literal("</td>")
        if link.weight is not None:
            text += h.literal("<td>%.1f%%</td>" % (link.weight*100))
        else:
            text += h.literal("<td>-</td>")
        text+=h.literal("</tr>")
        return text

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


    @classmethod
    def _construct_preliminary_mergertree(self, halo, base_halo, visited=None, depth=0):
        if visited is None:
            visited = []
        start = time.time()
        recurse = halo.id not in visited
        visited.append(halo.id)

        rl = db.hopper.HopStrategy(halo)
        rl.target_timestep(halo.timestep.previous)

        rl, weights = rl.all_and_weights()

        if len(rl)>0:
            rl = [rli for rli,wi in zip(rl,weights) if wi>weights[0]*0.02]

        timeinfo = "TS ...%s; z=%.2f; t=%.2e Gyr"%(halo.timestep.extension[-5:], halo.timestep.redshift, halo.timestep.time_gyr)

        if isinstance(halo, db.core.BH):
            mass_name = "BH_mass"
            moreinfo = "BH %d"%halo.halo_number
        else:
            mass_name = "Mvir"

            if halo.NDM>1e4:
                moreinfo = "%s %d, NDM=%.2e"%(halo.__class__.__name__,halo.halo_number, halo.NDM)
            else:
                moreinfo = "%s %d, NDM=%d"%(halo.__class__.__name__,halo.halo_number, halo.NDM)

        Mvir = halo.properties.filter_by(name_id=db.get_dict_id(mass_name, session=Session.object_session(halo))).first()
        if Mvir is not None:
            moreinfo+=", %s=%.2e"%(mass_name,Mvir.data)
            unscaled_size = math.log10(Mvir.data)
        else:
            unscaled_size = 1.0
        nodeclass = 'node-dot-standard'


        name = str(halo.halo_number)
        start = time.time()
        if halo.links.filter_by(relation_id=db.get_dict_id('Sub',-1)).count()>0:
            nodeclass = 'node-dot-sub'
        self.search_time+=time.time()-start
        if halo == base_halo:
            nodeclass = 'node-dot-selected'
        elif depth==0:
            if halo.next is not None:
                nodeclass = 'node-dot-continuation'
                name = '...'
                moreinfo = "Continues... "+moreinfo



        if len(name)>4:
            name = ""

        output = {'name': name ,
                  'url': url(controller="sims", action="mergertree", id=halo.id),
                  'nodeclass': nodeclass,
                  'moreinfo': moreinfo,
                  'timeinfo': timeinfo,
                  'halo_number': halo.halo_number,
                  'unscaled_size': unscaled_size ,
                  'contents': [],
                  'depth': depth }

        maxdepth = 0

        if recurse:
            for rli in rl:
                nx = self._construct_preliminary_mergertree(rli, base_halo,visited,depth+1)
                output['contents'].append(nx)
                if nx['maxdepth']>maxdepth: maxdepth = nx['maxdepth']

        output['maxdepth'] = maxdepth+1
        return output

    @classmethod
    def _visit_tree(self, tree):
        yield tree
        for subtree in tree['contents']:
            for item in self._visit_tree(subtree) : yield item


    @classmethod
    def _postprocess_megertree_rescale(cls, tree):
        max_size = -100
        for node in cls._visit_tree(tree):
            if node['unscaled_size']>max_size:
                max_size = node['unscaled_size']

        for node in cls._visit_tree(tree):
            size = 10+3*(node['unscaled_size']-max_size)
            if size<3:
                size = 3
            node['size'] = size


    @classmethod
    def _postprocess_mergertree(self, tree):
        self._postprocess_megertree_rescale(tree)
        self._postprocess_mergertree_layout_by_branch(tree)

    @classmethod
    def _postprocess_mergertree_layout_by_number(cls, tree, key='halo_number'):
        x_vals = [set() for i in xrange(tree['maxdepth'])]

        for node in cls._visit_tree(tree):
            x_vals[node['depth']].add(node[key])

        max_entries = max([len(v) for v in x_vals])
        x_map = [{} for i in xrange(tree['maxdepth'])]
        for this_vals, this_map in zip(x_vals, x_map):
            new_x = 15 * (max_entries - len(this_vals))
            for xv in sorted(this_vals):
                this_map[xv] = new_x
                new_x += 30

        for node in cls._visit_tree(tree):
            node['_x'] = x_map[node['depth']][node[key]]

    @classmethod
    def _postprocess_mergertree_layout_by_branch(cls, tree):

        tree['space_range'] = (0.0,1.0)
        existing_ranges = [{} for i in xrange(tree['maxdepth'])]
        for node in cls._visit_tree(tree):
            x_start, x_end = node['space_range']
            node['mid_range'] = (x_start+x_end)/2
            if len(node['contents'])>0:
                delta = (x_end-x_start)/len(node['contents'])
                for i,child in enumerate(node['contents']):
                    child_range = existing_ranges[child['depth']].get(child['halo_number'],
                                                                      (x_start + i *delta, x_start + (i+1)*delta))

                    child['space_range'] = child_range
                    existing_ranges[child['depth']][child['halo_number']]=child_range

        cls._postprocess_mergertree_layout_by_number(tree, 'mid_range')

    @classmethod
    def _construct_mergertree(self, halo):
        self.search_time=0
        start = time.time()
        base = halo
        for i in range(5):
            if base.next is not None:
                base = base.next

        tree = self._construct_preliminary_mergertree(base, halo)
        print "Merger tree build time:    %.2fs"%(time.time()-start)
        print "of which link search time: %.2fs"%(self.search_time)

        start = time.time()
        self._postprocess_mergertree(tree)
        print "Post-processing time: %.2fs"%(time.time()-start)

        """
        start = time.time()
        rl = db.hopper.MultiHopStrategy(halo, directed='backwards', nhops_max=1)
        rl.target_simulation(halo.timestep.simulation)
        print "size=",rl.count()
        print len(rl.all())
        print rl.all_weights_and_routes()[-1]
        print "one-query back: ",time.time()-start
        """

        return tree

    def mergertree(self, id, rel=None, num=1):
        halo,id = self._showhalo_prepare(id,rel,num)
        c.this_action = 'mergertree'
        c.alternative_action = 'showhalo'
        c.alternative_action_name = "&larr; halo properties"
        halo = Session.query(meta.Halo).filter_by(id=id).first()
        c.tree = self._construct_mergertree(halo)
        c.breadcrumbs = h.breadcrumbs(halo)
        return render('/sims/mergertree.mako')

    def _showhalo_prepare(self, id, rel=None, num=1):

        halo = Session.query(meta.Halo).filter_by(id=id).first()
        if not hasattr(c,'flash'):
            c.flash = []

        if rel is not None :
            num = int(num)

            new_halo = self._find_halo_relation(halo, num, rel)

            if new_halo is None and c.flash==[]:
                c.flash = [h.literal("Unable to find a suitable halo &ndash; either it doesn't exist, or the linking information is missing.")]

            if new_halo is not None:
                halo = new_halo
                id = new_halo.id

        c.name = "Halo "+str(halo.halo_number)+" of "+halo.timestep.extension
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
        c.halo_number = halo.halo_number


        c.breadcrumbs = h.breadcrumbs(halo)

        return halo, id

    def showhalo(self, id, rel=None,num=1) :
        halo,id = self._showhalo_prepare(id,rel,num)
        c.this_action = 'showhalo'
        c.alternative_action = 'mergertree'
        c.alternative_action_name = "merger tree &rarr;"
        c.props = self._render_properties(halo.properties, parents=True)
        c.dep_props = self._render_properties(halo.deprecated_properties, links=False, parents=True)
        c.bh = self._render_bh(halo.links, halo.reverse_links)
        c.links = self._render_links(halo.links, halo.reverse_links)

        return render('/sims/showhalo.mako')

    def _find_halo_relation(self, halo, num, rel):
        if rel == "earlier":
            new_halo = halo
            for i in xrange(num):
                if new_halo.previous is not None:
                    new_halo = new_halo.previous
            if new_halo is not None and new_halo.id == halo.id:
                c.flash = ["You're already looking at the earliest snapshot for this halo"]
        elif rel == "earliest":
            new_halo = halo.earliest
            if new_halo is not None and new_halo.id == halo.id:
                c.flash = ["You're already looking at the earliest snapshot for this halo"]
        elif rel == "later":
            new_halo = halo
            for i in xrange(num):
                if new_halo.next is not None:
                    new_halo = new_halo.next
            if new_halo is not None and new_halo.id == halo.id:
                c.flash = ["You're already looking at the latest snapshot for this halo"]
        elif rel == "latest":
            new_halo = halo.latest
            if new_halo is not None and new_halo.id == halo.id:
                c.flash = ["You're already looking at the latest snapshot for this halo"]
        elif rel == "other_in_ts":
            new_halo = halo.timestep.halos.filter_by(halo_number=num).first()
        elif rel == "insim":
            targ = Session.query(meta.Simulation).filter_by(id=num).first()
            strategy = db.hopper.MultiHopStrategy(halo, MAXHOPS_FIND_HALO, 'across')
            strategy.target(targ)

            targets, weights = strategy.all_and_weights()

            if len(targets) == 0:
                new_halo = None
            else:
                new_halo = targets[0]
                message = "Confidence %.1f%%" % (100 * weights[0])
                if len(targets) > 1 and weights[1] is not None:
                    message += h.literal("; the next candidate is %s and has confidence %.1f%%" % (self._relative_link(targets[0],targets[1]), 100 * weights[1]))
                c.flash = [message]

        else:
            c.flash = ["I have never heard of the relationship %r, so can't find the halo for you" % rel]
        return new_halo
