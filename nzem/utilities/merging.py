import pandas as pd

def series_merge(self, other, **kwds):
    """
    Merge two series, on their indices, with one another to create a dataframe
    
    Parameters
    ----------
    self : The first series to be applied
    other : The second series
    **kwds : Any additional merge key word arguments (e.g. suffixes etc)
    
    Returns
    -------
    DataFrame : Pandas DataFrame containing the joined data
    """
    if type(other) in (pd.core.series.Series, pd.core.series.TimeSeries):
        return pd.DataFrame({self.name: self}).merge(pd.DataFrame({other.name:
                             other}), left_index=True, right_index=True, **kwds)
    else:
        raise TypeError("You must pass a series to the merge series object, " \
                        "You have passed an object of type:", type(other)) 
         
def merge_dfseries(self, series, **kwds):
    """
    Merge a DataFrame and a series together
    
    Parameters
    ----------
    self : DataFrame to which the series is to be added
    series : Series to be merged with the DataFrame
    **kwds : Merge key word arguments
    
    Returns
    -------
    DataFrame : A new DataFrame with the series merged in based upon its index
    """
    if type(series) in (pd.core.series.Series, pd.core.series.TimeSeries):
        return self.merge(pd.DataFrame({series.name: series}), right_index=True, 
                         **kwds)
    else:
        raise TypeError("You must pass a series to the merge series object, " \
                        "You have passed an object of type:", type(series)) 
                            
# Apply to the respective classes
pd.DataFrame.merge_series = merge_dfseries
pd.Series.merge_series = series_merge
