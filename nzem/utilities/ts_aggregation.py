"""
A range of small helper functions to make applying time series aggregation easier.
In general these may be applied to groupby functions e.g. 

a = df.groupby(ts_agg)

And are used in conjunction with aggregation functions
"""

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def ts_aggregation(df, stat_col, ts_agg=None, agg=None, **kargs):
    """ Perform statistics upon monthly data, applies the stat function
    Defaults to the mean
    
    Parameters
    ----------
    df : to be transitioned
    stat_col : Column for stats to be applied to
    ts_agg : Aggregation function to be applied to the DF, defaults to
             day of year
    agg : Function to be applied (many to one)
    **kargs : additional key word arguments for the aggregation function
    
    Returns
    -------
    monthly : Series containing monthly data
    """

    g = df.groupby(ts_agg) if ts_agg else df.groupby(lambda x: x.dayofyear)
    return g[stat_col].aggregate(agg, **kargs) if agg else g[stat_col].mean()
    

def dom_tsagg(x):
    return x.day
    
def dow_tsagg(x):
    return x.weekday()
    
def woy_tsagg(x):
    return x.weekofyear
    
def moy_tsagg(x):
    return x.month
    
def year_tsagg(x):
    return x.year
    
def doy_tsagg(x):
    return x.dayofyear
    
def tp_tsagg(x):
    return 48  if x.hour == 0 and x.minute == 0 else x.hour * 2 + x.minute / 30
    
# Note, best way to use is via a lambda function with set and o in groupby
# Groupby doesn't take kwargs
def season_tsagg(x, s=3., o=1.):
    if x.month + o < 13:
        return np.ceil((x.month + o) / float(s)) 
    else:
        return np.ceil((x.month + o - 12) / float(s))
    

    


if __name__ == '__main__':
    pass
