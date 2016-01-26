import os

def ahf_stat_name(ts):
    """Return the AHF halo filename corresponding to the specified timestep"""
    return ts.filename+'.z%.3f.AHF_halos'%(ts.redshift)

def iter_halostat(ts, *args):
    if os.path.exists(ts.filename+".amiga.stat"):
        for item in iter_stat_file(ts.filename+".amiga.stat",*args):
            yield item
    elif os.path.exists(ahf_stat_name(ts)):
        kwa = {"id_offset":1}
        for item in iter_stat_file(ahf_stat_name(ts),*args,**kwa):
            yield item
    else:
        raise IOError, "Cannot find a suitable .stat file"

def iter_stat_file(fname, *args, **kwargs):
    """Yield the halo ID and requested columns from each line of the stat file.

    *yields*:
        id, arg1, arg2, arg3 where ID is the halo ID and argN is the value of the Nth named column

    *kwargs*:
        id_offset - (default 0) The halo ID is offset by the specified amount

    """

    id_offset = kwargs.get('id_offset',0)

    with open(fname) as f:
        header = [x.split("(")[0] for x in f.readline().split()]
        ids = [0]+[header.index(a) for a in args]
        for l in f:
            results = []
            l_split = l.split()
            for id_this in ids:
                this_str = l_split[id_this]
                if "." in this_str:
                    guess_type = float
                else :
                    guess_type = int

                try:
                    this_cast = guess_type(this_str)
                except ValueError:
                    this_cast = this_str

                results.append(this_cast)

            results[0]+=id_offset

            yield results
