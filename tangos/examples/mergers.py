from __future__ import absolute_import
import tangos as db
import tangos.relation_finding
import numpy as np


def get_mergers_of_major_progenitor(input_halo):
    """Given a halo, return the redshifts and ratios of all mergers on the major progenitor branch

    :parameter input_halo - the halo to consider the history of
    :type input_halo tangos.core.Halo

    :returns redshift, ratio, halo - arrays of the redshifts, ratios (1:X) and halo DB objects for the mergers
    """
    redshift = []
    ratio = []
    halo  = []
    while input_halo is not None:
        mergers = db.relation_finding.MultiHopMostRecentMergerStrategy(input_halo).all()
        if len(mergers)>0 :
            for m in mergers[1:]:
                redshift.append(mergers[0].timestep.next.redshift)
                halo.append((mergers[0], m))
                ratio.append(float(mergers[0].NDM)/m.NDM)
            input_halo = mergers[0]
        else:
            input_halo = None

    return np.array(redshift), np.array(ratio), halo

def most_major_mergers_since(ts, Mvir_min=0.8e12, Mvir_max=2e12, z_merger_max=4.0, no_merger_value=None):
    """Given a timestep, calculate the most major merger ratio since a given redshift for all halos in a specified mass range

    :parameter ts - The (end) timestep to take halos from
    :parameter Mvir_min, Mvir_max - the minimum and maximum halo masses to consider
    :parameter z_merger_max - the maximum redshift to search for mergers
    :parameter no_merger_value - the value to insert for a halo if no merger was found (default None)
    :type ts tangos.core.TimeStep

    """
    dbid, Mvir = ts.calculate_all("dbid()","Mvir")
    mask = (Mvir>Mvir_min)*(Mvir<Mvir_max)

    most_major = []
    for current_id in dbid[mask]:
        halo = db.get_halo(current_id)
        redshift, ratio, halos = get_mergers_of_major_progenitor(halo)
        m_mask = np.array(redshift)<z_merger_max
        if m_mask.sum()>0:
            most_major.append(min(ratio[m_mask]))
        else:
            most_major.append(no_merger_value)
    return np.array(most_major)
