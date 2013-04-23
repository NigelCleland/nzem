import pandas as pd
from datetime import datetime, timedelta

def niwa_parse(x, parse_str="%d%b%y"):
    d = datetime.strptime(x, parse_str)
    return d if d.year <= 2020 else datetime(d.year - 100, d.month, d.day)

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
    
if __name__ == '__main__':
    pass
    
    
    
    
