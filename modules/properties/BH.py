from . import HaloProperties, TimeChunkedProperty
import numpy as np
import math
import pynbody
import re
import scipy, scipy.interpolate
import weakref

class BHShortenedLog(self):
    _cache = {}
    def __new__(self, f, filename):
        obj = _cache.get(filename, lambda: None)()
        if obj is not None:
            return obj
        obj = object.__new__(BHShortenedLog, f, filename)
        _cache[filename] = weakref.ref(obj)

    def __init__(self, f, filename):
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$",filename).groups()
        ars = [[] for i in range(14)]
        for line in open(name+".shortened.orbit"):
            line_split = line.split()
            ars[0].append(int(line_split[0]))
            for i in range(1,len(line_split)):
                ars[i].append(float(line_split[i]))


        wrapped_ars = [pynbody.array.SimArray(x) for x in ars]
        for w in wrapped_ars:
            w.sim = f
        #bhid, time, step, mass, x, y, z, vx, vy, vz, pot, mdot, deltaM, E, dtEff, scalefac = wrapped_ars
        bhid, time, step, mass, x, y, z, vx, vy, vz, mdot, mdotmean, mdotsig, scalefac = wrapped_ars
        bhid = np.array(bhid,dtype=int)
        print len(time),"entries"

        munits = f.infer_original_units("Msol")
        posunits = f.infer_original_units("kpc")
        velunits = f.infer_original_units("km s^-1")
        #potunits = velunits**2
        tunits = posunits/velunits
        #Eunits = munits*potunits
        # decorate with units

        mass.units = munits
        x.units = y.units = z.units = posunits
        vx.units = vy.units = vz.units = velunits
        #pot.units = potunits
        mdot.units = munits/tunits
        time.units = tunits
        #E.units = Eunits


        x.convert_units('kpc')
        y.convert_units('kpc')
        z.convert_units('kpc')
        vx.convert_units('km s^-1')
        vy.convert_units('km s^-1')
        vz.convert_units('km s^-1')
        mdot.convert_units('Msol yr^-1')
        mass.convert_units("Msol")
        time.convert_units("Gyr")
        #E.convert_units('erg')


        self.vars = {'bhid':bhid, 'step':step, 'x':x, 'y':y, 'z':z,
                    'vx':vx, 'vy':vy, 'vz': vz, 'mdot': mdot, 'mdotmean':mdotmean,'mdotsig':mdotsig, 'mass': mass,
                     'time': time}


    def get_at_stepnum(self, stepnum):
        mask = self.vars['step']==stepnum
        return dict((k,v[mask]) for k,v in self.vars.iteritems())

    def get_for_named_snapshot(self, filename):
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$",filename).groups()
        stepnum = int(stepnum)
        return get_at_stepnum(self, stepnum)


class BH(HaloProperties):

    def name(self):
        return "BH_mdot", "BH_mdot_ave", "BH_mdot_std", "BH_central_offset", "BH_central_distance", "BH_mass"

    def requires_property(self):
        return []

    def no_proxies(self):
        return True

    def preloop(self, f, filename, pa):
        self.log = BHShortenedLog(f,filename)
        self.filename = filename


    def calculate(self, halo, properties):
        import halo_db as db
        if not isinstance(properties, db.Halo):
            raise RuntimeError("No proxies, please")

        if len(halo)!=1:
            raise RuntimeError("Not a BH!")

        if halo['tform'][0]>0:
            raise RuntimeError("Not a BH!")

        vars = self.log.get_for_named_snapshot(self.filename)

        mask = vars['bhid']==halo['iord']
        if(mask.sum()==0):
            raise RuntimeError("Can't find BH in .orbit file")

        # work out who's the main halo
        main_halo = None
        for i in properties.reverse_links:
            if i.relation.text.startswith("BH"):
                main_halo = i.halo_from
                break
        if main_halo is None:
            raise RuntimeError("Can't relate BH to its parent halo")
        print "Main halo is:", main_halo

        main_halo_ssc = main_halo['SSC']

        entry = np.where(mask)[0]

        print "target entry is",entry
        final = {}
        for t in 'x','y','z','vx','vy','vz','mdot', 'mass', 'mdotmean','mdotsig':
            final[t] = float(vars[t][entry])

        offset = np.array((final['x'],final['y'],final['z']))-main_halo_ssc

        return final['mdot'], final['mdotmean'], final['mdotsig'], offset, np.linalg.norm(offset), final['mass']


class BHAccHistogram(TimeChunkedProperty):
    def name(self):
        return "BH_mdot_histogram"

    def preloop(self, f, filename, pa):
        self.log = BHShortenedLog(f,filename)

    def no_proxies(self):
        return True

    def calculate(self, halo, properties):

        if len(halo)!=1:
            raise RuntimeError("Not a BH!")

        if halo['tform'][0]>0:
            raise RuntimeError("Not a BH!")

        mask = self.log.vars['bhid']==halo['iord']
        if(mask.sum()==0):
            raise RuntimeError("Can't find BH in .orbit file")

        t_orbit = self.log.vars['time']
        Mdot_orbit = self.log.vars['Mdot']

        t_max = properties.timestep.time_gyr
        t_grid = np.arange(0, self.tmax_Gyr, self.nbins)
        Mdot_grid = scipy.interpolate.interp1d(t_orbit, Mdot_orbit)(t_grid)
        return Mdot_grid[self.store_slice(t_max)]