import pandas as pd

def monthly_agg(df, stat_col, agg=None):
    """ Perform statistics upon monthly data, applies the stat function
    Defaults to the mean
    
    Parameters
    ----------
    df : to be transitioned
    stat_col : Column for stats to be applied to
    agg : Function to be applied (many to one)
    
    Returns
    -------
    monthly : Series containing monthly data
    """

    g = df.groupby(lambda x: x.month)
    return g[stat_col].aggregate(agg) if agg else g[stat_col].mean()
