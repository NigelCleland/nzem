import pandas as pd

def ts_agg(df, stat_col, ts_agg=None, agg=None, **kargs):
    """ Perform statistics upon monthly data, applies the stat function
    Defaults to the mean
    
    Parameters
    ----------
    df : to be transitioned
    stat_col : Column for stats to be applied to
    agg : Function to be applied (many to one)
    **kargs : additional key word arguments for the aggregation function
    
    Returns
    -------
    monthly : Series containing monthly data
    """

    g = df.groupby(ts_agg)
    return g[stat_col].aggregate(agg, **kargs) if agg else g[stat_col].mean()
    
    
    
    
