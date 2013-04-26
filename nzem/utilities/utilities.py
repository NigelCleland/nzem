import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def niwa_parse(x, parse_str="%d%b%y"):
    d = datetime.strptime(x, parse_str)
    return d if d.year <= 2020 else datetime(d.year - 100, d.month, d.day)
    
def merge_dfseries(df, series,**kargs):
    return df.merge(pd.DataFrame({series.name: series}), 
                    right_index=True, **kargs)
                    
def merge_series(s1, s2):
    return pd.DataFrame({s1.name: s1}).merge(pd.DataFrame({s2.name: s2}),
        left_index=True, right_index=True)
        
        
def cdf(series, step=1):
    """
    Construct a cumulative density function for a series and return the
    results as a pandas Series
    
    Parameter
    ---------
    series : The series of data for which a CDF should be calculated
    step : Step size to be applied
    
    Returns
    -------
    cdf_series : A series containing the CDF for the input series
    """
    
    binrange = np.arange(series.min()-step, series.max()+step, step)
    hi, be = np.histogram(series, bins=binrange)
    norm = 1 - np.cumsum(1. * hi / hi.sum())
    return pd.Series(norm, index=be[:-1])
    
def conditional_probability(s1, s2, step=1, how='ge'):
    """
    Construct a conditional probability between two series where one series
    is a strict subset of the second series
    """
    
    binrange = np.arange(s2.min()-step, s2.max()+step, step)
    return cumulative_frequency(s1, s2, binrange, how=how).sort_index()
    
def cumulative_frequency(s1, s2, binrange, how='ge'):
    """
    Determine the cumulative frequency plots for a binrange across two series
    Here s1 is a strict subset of s2
    """

    
    f = (1. * s1.mask(b, how=how).count() / s2.mask(b, how=how).count() for
        b in binrange)
    return pd.Series(f, binrange)
    
    
    

if __name__ == '__main__':
    pass
    
    
    
    
