#!/usr/bin/env python

import Simpy.BlackHoles.orbit, Simpy.BlackHoles.mergers
import sys

#Simpy.BlackHoles.orbit.sepOrbitbyStep(sys.argv[1], minstep = 1, maxstep = 4096)
#Simpy.BlackHoles.orbit.truncOrbitFile(sys.argv[1], minstep = 1, maxstep = 4096) 
Simpy.BlackHoles.mergers.reducedata(sys.argv[1],outname='changa.out*',mergename='BHmerge.txt',NCHILADA=False)
