from . import HaloProperties
import numpy as np
import math
import pynbody
import re

class BH(HaloProperties):

    def name(self):
        return "BH_accrate", "BH_central_offset", "BH_central_distance", "BH_mass"

    def requires_property(self):
        return []

    def no_proxies(self):
        return True

    def preloop(self, f, filename, pa):
        name, stepnum = re.match("^(.*)\.(0[0-9]*)$",filename).groups()
        stepnum = float(stepnum)

        ars = [[] for i in range(16)]
        print name, stepnum
        for line in open(name+".orbit"):
            line_split = line.split()
            stepnum_line = float(line_split[2])
            if stepnum_line>stepnum-1 and stepnum_line<stepnum+1:
                ars[0].append(int(line_split[0]))
                for i in range(1,len(line_split)):
                    ars[i].append(float(line_split[i]))


        wrapped_ars = [pynbody.array.SimArray(x) for x in ars]
        for w in wrapped_ars:
            w.sim = f
        bhid, time, step, mass, x, y, z, vx, vy, vz, pot, mdot, deltaM, E, dtEff, scalefac = wrapped_ars
        bhid = np.array(bhid,dtype=int)
        print len(time),"entries"

        munits = f.infer_original_units("Msol")
        posunits = f.infer_original_units("kpc")
        velunits = f.infer_original_units("km s^-1")
        potunits = velunits**2
        tunits = posunits/velunits
        Eunits = munits*potunits
        # decorate with units

        mass.units = munits
        x.units = y.units = z.units = posunits
        vx.units = vy.units = vz.units = velunits
        pot.units = potunits
        mdot.units = munits/tunits
        E.units = Eunits


        x.convert_units('kpc')
        y.convert_units('kpc')
        z.convert_units('kpc')
        vx.convert_units('km s^-1')
        vy.convert_units('km s^-1')
        vz.convert_units('km s^-1')
        mdot.convert_units('Msol yr^-1')
        mass.convert_units("Msol")
        E.convert_units('erg')


        self.vars = {'bhid':bhid, 'step':step, 'x':x, 'y':y, 'z':z,
                    'vx':vx, 'vy':vy, 'vz': vz, 'mdot': mdot, 'E': E , 'mass': mass}

        self.stepnum = stepnum


    def calculate(self, halo, properties):
        import halo_db as db
        if not isinstance(properties, db.Halo):
            raise RuntimeError("No proxies, please")

        if len(halo)!=1:
            raise RuntimeError("Not a BH!")

        if halo['tform'][0]>0:
            raise RuntimeError("Not a BH!")

        fl = self.vars['bhid']==halo['iord']
        if(fl.sum()==0):
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

        entry = np.where(fl)[0][np.argmin(abs(self.stepnum-self.vars['step'][fl]))]

        print "target entry is",entry
        final = {}
        for t in 'x','y','z','vx','vy','vz','mdot','E', 'mass':
            final[t] = self.vars[t][entry]
            print t,final[t]

        offset = np.array((final['x'],final['y'],final['z']))-main_halo_ssc

        return final['mdot'], offset, np.linalg.norm(offset), final['mass']
