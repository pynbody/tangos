import logging

from pylons import request, response, session, tmpl_context as c, url
from pylons.controllers.util import abort, redirect

from simdb.lib.base import BaseController, render

import simdb.lib.helpers as h

import matplotlib, random
matplotlib.use('Agg')
import pylab as p
import PIL, PIL.Image, StringIO, threading
import numpy as np
import sys
import properties

imageThreadLock = threading.Lock()

log = logging.getLogger(__name__)

import simdb.model.meta as meta
from simdb.model.meta import *

import formalchemy

from sqlalchemy.sql import and_, or_

class nocontext :
    def __enter__(self) :
        pass

    def __exit__(self, type, value, tb) :
        pass

nocontext = nocontext()

# plotopt_fieldset = formalchemy.FieldSet(meta.ArrayPlotOptions)

class PlotController(BaseController):



    def start(self) :
        self.canvas =  p.get_current_fig_manager().canvas
        response.content_type = 'image/png'

    def finish(self, getImage=True) :

        if getImage :
            self.canvas.draw()
            imageSize = self.canvas.get_width_height()
            imageRgb = self.canvas.tostring_rgb()
            buffer = StringIO.StringIO()
            pilImage = PIL.Image.fromstring("RGB",imageSize, imageRgb)
            pilImage.save(buffer, "PNG")

        p.close()

        if getImage :
            return buffer.getvalue()


    def plotopt(self, id) :
        return_id = request.params['return_array_id']
        prop = Session.query(meta.DictionaryItem).filter_by(id=id).first()
        c.name = prop.text
        c.id = prop.id
        c.return_id = return_id

        try :
            item = prop.array_plot_options[0]
        except IndexError :
            item = meta.ArrayPlotOptions

        c.fs = formalchemy.FieldSet(item, data=request.POST or None)


        if request.POST and c.fs.validate() :
            c.fs.sync()
            prop = Session.query(meta.DictionaryItem).filter_by(id=id).first()
            c.fs.model.relates_to = prop
            Session.commit()
            redirect(url(controller='plot', action='array', id=return_id))


        c.fs.configure()
        c.fs.model.relates_to = prop

        del c.fs.relates_to


        return render("/plots/plotopt_ajax.mako")


    def array(self, id, rel=None) :


        c.id = id

        prop = Session.query(meta.HaloProperty).filter_by(id=id).first()

        if rel is not None :
            halo = prop.halo
            prop2 = None
            # skip through to see where the next halo with this property is
            while halo is not None and prop2 is None :
                if rel=="next" :
                    halo = halo.next
                elif rel=="previous" :
                    halo = halo.previous
                if halo is not None :
                    prop2 = halo.properties.filter_by(halo_id=halo.id,name_id=prop.name_id).first()
            if prop2 is not None :
                redirect(url(controller='plot',action='array',id=prop2.id))
                return
            else :
                c.flash = ['Unavailable']

        c.breadcrumbs = h.breadcrumbs(prop)
        c.name = prop.name.text
        c.haloid = prop.halo_id
        c.dictid = prop.name.id


        try :
            item = prop.name.array_plot_options[0]
        except IndexError :
            item = meta.ArrayPlotOptions

        c.fs = formalchemy.FieldSet(item)
        c.fs.configure()
        c.fs.model.relates_to = prop.name
        del c.fs.relates_to

        if "overplot" in request.params :
            c.image_url = url(controller='plot', action='array_img', id=c.id, overplot=request.params["overplot"])
        else :
            c.image_url = url(controller='plot', action='array_img', id=c.id)


        return render("/plots/array.mako")

    def time(self, id) :
        c.id = id

        prop = Session.query(meta.HaloProperty).filter_by(id=id).first()
        c.breadcrumbs = h.breadcrumbs(prop)

        c.name = prop.name.text
        c.haloid = prop.halo_id
        c.dictid = prop.name.id
        return render("/plots/time.mako")


    def time_img(self, id) :
        print "HID = ",id
        prop = Session.query(meta.HaloProperty).filter_by(id=id).first()
        assert(type(prop.data) is float)

        redshift = []
        px = []
        spoint = prop.halo.earliest
        redshift, px = spoint.property_cascade("t()",prop.name.text)

        """while spoint!=None :
            prop_tv = Session.query(meta.HaloProperty).filter_by(halo_id=spoint.id, name_id=prop.name_id).order_by(meta.HaloProperty.id.desc()).first()
            if prop_tv!=None :
                redshift.append(spoint.timestep.redshift)
                px.append(prop_tv.data)

            spoint = spoint.next
        """

        with imageThreadLock :
            self.start()

            p.plot(redshift,px)
            p.xlabel("Time/Gyr")
            p.ylabel(prop.name.text)
            return self.finish()


    def raw_dat(self, id) :
        response.content_type='text/txt'
        ts=""
        prop = Session.query(meta.HaloProperty).filter_by(id=id).first()
        ts+="# Raw data from simdb\n"
        ts+="# %s for halo %d of %s"%(prop.name.text,prop.halo.halo_number,prop.halo.timestep.relative_filename)
        ts+="""# The class provides the following plot information:
#
        """
        for x in prop.data :
            ts+=str(x)+"\n"

        return ts



    def image_img(self, id) :
        halo_id = request.params.get('halo_id')
        log = request.params.get('image_log')
        prop = Session.query(meta.HaloProperty).filter_by(id=id).first()
        name_id = prop.name_id

        if halo_id!=prop.halo_id:
            prop = Session.query(meta.HaloProperty).filter_by(name_id=name_id,halo_id=halo_id).first()

        print "image_img",id,log
        if len(prop.data.shape)==1 :
            return self.array_img(id,prop=prop)
        with imageThreadLock :
            self.start()
            cl = properties.providing_class(prop.name.text)(prop.halo.timestep.simulation)
            width = cl.plot_extent()
            if log:
                data = np.log10(prop.data)
                data[data!=data]=data[data==data].min()

            else:
                data = prop.data

            print data.min(),data.max(),width
            if width is not None :
                p.imshow(data,extent=(-width/2,width/2,-width/2,width/2))
            else :
                p.imshow(data)

            p.xlabel(cl.plot_xlabel())
            p.ylabel(cl.plot_ylabel())

            if len(prop.data.shape) is 2 :
                cb = p.colorbar()
                if cl.plot_clabel() :
                    cb.set_label(cl.plot_clabel())

            return self.finish()

    def halo_img(self, id) :
        prop = Session.query(meta.HaloProperty).filter_by(id=id).first()
        assert(type(prop.data) is float)

        nx = []
        px = []
        spoint = prop.halo.timestep_id

        props = Session.query(meta.HaloProperty).join(meta.Halo).filter(and_(meta.Halo.timestep_id==spoint, meta.HaloProperty.name_id==prop.name_id)).all()

        # filter(and_(meta.Halo.timestep_id==spoint, meta.HaloProperty.name_id==prop.name_id)).all()

        for z in props :
            nx.append(z.halo.halo_number)
            px.append(z.data)

        with imageThreadLock :
            self.start()
            p.plot(nx,px,"x")
            return self.finish()

    def xy_img(self, id="img") :
        form = id
        did1 = str(request.params['x'])
        did2 = str(request.params['y'])
        if request.params['x_array_element']!="":
            did1="at("+request.params['x_array_element']+","+did1+")"

        if request.params['y_array_element']!="":
            did2 = "at(" + request.params['y_array_element'] + "," + did2 + ")"

        nosubs = request.params.get('nosubs')=='on'

        halo_id = int(request.params['halo_id'])

        h = Session.query(meta.Halo).filter_by(id=halo_id).first()
        ts = h.timestep

        xlog = (request.params.get('xlog'))=='on'
        ylog = request.params.get('ylog')=='on'
        print>>sys.stderr, "Making plot..."

        with imageThreadLock :
            if form == "img" :
                self.start()
                p.clf()

            if request.params['type']!='thistimestep' :
                # gather for this halo at all timesteps
                h_start = h.earliest
                x_vals, y_vals = h_start.property_cascade(did1,did2)
                if form=="img" :
                    p.plot(x_vals, y_vals,'k')
                    p.plot([h.calculate(did1)],
                           [h.calculate(did2)],'ro')

            else :
                if nosubs :
                    x_vals, y_vals = ts.gather_property(did1,did2,filt=True)
                else :
                    x_vals, y_vals = ts.gather_property(did1,did2)
                if form=="img" :
                    p.plot(np.array(x_vals),np.array(y_vals),"k.")


            if form == "img" :
                p.xlabel(did1)
                p.ylabel(did2)
                if xlog and ylog :
                    p.loglog()
                elif xlog :
                    p.semilogx()
                elif ylog :
                    p.semilogy()
                return self.finish()
            else :
                response.content_type = 'text/txt'
                if request.params['type']=='thistimestep' :
                    resp = "# Data from timestep "+ts.relative_filename
                    if nosubs :
                        resp += "\n# Subhalos have been excluded if the subhalo information is available"
                else :
                    resp = "# Data from halo "+h.timestep.relative_filename+" halo "+str(h.halo_number)+" tracking across time"
                resp += "\n# %s %s\n"%(did1,did2)
                for x,y in zip(x_vals, y_vals) :
                    resp+= "%.5g %.5g\n"%(x,y)
                return resp


    def array_img(self, id, overplot=False, prop=None):
        text = (request.params.get('text')) is not None


        if text :
            response.content_type='text/txt'

        with imageThreadLock if not overplot else nocontext :
            if prop is None :
                prop = Session.query(meta.HaloProperty).filter_by(id=id).first()
            assert isinstance(prop, meta.HaloProperty)

            if text :
                h = prop.halo
                ts = "# Array "+prop.name.text+" for "+h.timestep.relative_filename+" halo "+str(h.halo_number)+"\n"

                data = prop.data
                xdat = prop.x_values()

                for x,y in zip(xdat,data) :
                    ts+="%.5g %.5g\n"%(x,y)
            else :
                if not overplot:
                    self.start()
                property_info = prop.name.providing_class()(prop.halo.timestep.simulation)
                index = property_info.index_of_name(prop.name.text)
                prop.plot()

                if property_info.plot_xlog() and property_info.plot_ylog() :
                    p.loglog()
                elif property_info.plot_xlog() :
                    p.semilogx()
                elif property_info.plot_ylog() :
                    p.semilogy()

                if property_info.plot_xlabel() :
                    p.xlabel(property_info.plot_xlabel())

                if property_info.plot_ylabel() :
                    p.ylabel(property_info.plot_ylabel()[index])

                if property_info.plot_yrange():

                    p.ylim(*property_info.plot_yrange())

            if overplot :
                return


            if "overplot" in request.params and not overplot :
                self.array_img(int(request.params["overplot"]),True)






            p.grid()
            p.title("z=%.2f, t=%.2e Gyr, halo %d"%(prop.halo.timestep.redshift, prop.halo.timestep.time_gyr, prop.halo.halo_number))
            if not text :
                return self.finish()
            else :
                return ts
