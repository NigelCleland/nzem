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
    
def conditional_sample_size(s, step=1, how='ge'):
    """
    Develop a conditional sample size for a particular series.
    Takes as optional arguments a method and step size
    """
    binrange = np.arange(s.min() - step, s.max() + step, step)
    f = (s.mask(b, how=how).count() for b in binrange)
    return pd.Series(f, binrange)
    
def conditional_sample_weight(s, step=1, how='ge'):
    
    samples = conditional_sample_size(s, step=step, how=how)
    return 1 - samples / float(samples.max())
    
def reduced_aggregation(series, npoints=500, agg=None, percent=True):
    """
    Perform a backward reduced aggregation calculation.
    Will sort the data, then take in fractionally reducing subsets the aggregation
    applied to that data.
    This enables visualisation of the effect of the extremes on the average 
    prices as straight averages are misleading.
    
    Parameters
    ----------
    series : The series to perform the reduced average on
    npoints : Number of points to perform it for
    agg : Aggregation to perform
    percent : Wether the series index should be a percentage figure
    
    Returns
    -------
    series : Series containing information regarding the applied aggregation
    """
    
    total_points = len(series)
    if npoints >= total_points:
        npoints = total_points - 1
        
    s = series.copy()
    s.sort()
    z = np.arange(1, npoints+1)
    m = [agg(s.values[:-i]) for i in z]
    if percent:
        z = z * 100. / total_points
    return pd.Series(m, index=z)
    
def point_reduced_aggregation(series, percent, agg=None):
    """
    A single point reduced average.
    Pass a percentage figure, will convert this into a subset of the data (e.g.
    passing 5 will remove the top 5% of the series points and then apply an 
    aggregation to them.
    
    Parameters
    ----------
    series : The pandas series to apply the transformation to
    percent : The percentage figure (notes, is reverse e.g. 5 = 95% of data)
    agg : The aggregation to apply
    
    Returns
    -------
    float : A float containig the applied aggregation
    """
    # Convert percentage to datapoint
    p = np.ceil(percent / 100.  * len(series))
    s = series.copy()
    s.sort()
    return agg(s.values[:-p]) if p > 0 else agg(s.values)

if __name__ == '__main__':
    pass
    
    
    
    
