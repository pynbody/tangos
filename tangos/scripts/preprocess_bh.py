#!/usr/bin/env python
from __future__ import absolute_import
import matplotlib
matplotlib.use('agg')

import Simpy.BlackHoles.orbit, Simpy.BlackHoles.mergers
import sys

def main():
    Simpy.BlackHoles.orbit.sepOrbitbyStep(sys.argv[1], minstep = 1, maxstep = 4096, NCHILADA=False)
    Simpy.BlackHoles.orbit.truncOrbitFile(sys.argv[1], minstep = 1, maxstep = 4096)
    Simpy.BlackHoles.mergers.reducedata(sys.argv[1],outname='changa.out*',mergename='BHmerge.txt',NCHILADA=False)
