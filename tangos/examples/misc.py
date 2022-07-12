import tangos as db
from .. import core
import numpy as np
import re


def timestep_index(self,tstep,**kwargs):
    '''
    Returns the index of the timestep in the database
    self - the simulation
    t - timestep; can be string, int, float, or timestep object

    kwargs
    prefix - what precedes the number of the timestep (e.g., DD, RD) (default: 'DD')
    '''

    if 'prefix' in kwargs:
        prefix = kwargs['prefix']
    else:
        prefix = 'DD'

    tslist = self.timesteps
    if any([isinstance(tstep,float),isinstance(tstep,int)]):
        tstr = prefix+str(int(tstep)).zfill(4)
    elif isinstance(tstep,str):
        condmatch = [bool(re.fullmatch('(RD|DD)([0-9]*)',tstep)),bool(re.fullmatch('(RD|DD)[0-9]*/(?:RD|DD)([0-9]*)',tstep)),bool(re.fullmatch(self.basename+'/(RD|DD)[0-9]*/(?:RD|DD)([0-9]*)',tstep))]
        if any(condmatch):
            tstr = tstep
        elif prefix not in tstep:
            if len(tstep)<5:
                tstr = prefix+tstep.zfill(4)
            else:
                raise AssertionError('Format of timestep not understood')
        else:
            raise AssertionError('Format of timestep not understood')
    elif (isinstance(tstep,core.timestep.TimeStep)):
        tstr = str(tstep).split()[1][1:-1]
    else:
        raise AssertionError('Format of timestep not understood')
    return [curind for curind, s in enumerate(tslist) if tstr in str(s)][0]
