import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import glob
import nzem
from dateutil.parser import parse
from datetime import datetime, timedelta

def parse_niwa_date(x):
    """ Parse the custom niwa date format """
    d = datetime.strptime(x, "%d%b%y")
    return d if d.year <= 2020 else datetime(d.year - 100, d.month, d.day)


def load_hydro_data(csv_name=None, date="Trading Date", parse_dates=True,
                    date_index=True, niwa_date=True):
    """
    Load the hydrology data with optional date and index handling.
    
    Parameters
    ----------
    csv_name : Name of the file to be loaded as hydro data
    date : String containing the trading date name
    parse_dates : Boolean, whether to attempt to parse the dates
    date_index : Boolean, should the date be assigned to the index
    
    Returns
    -------
    lake_storage : DataFrame containing the lake storage information
    """    
    
    if csv_name == None:
        csv_name = os.getcwd() +  '/nzem/maps/Hydro_Lake_Data.csv'
    
    lake_storage = pd.read_csv(csv_name)
    if parse_dates:
        if niwa_date:
            lake_storage[date] = lake_storage[date].apply(parse_niwa_date)
        else:
            lake_storage[date] = lake_storage[date].apply(lambda x: parse(x))
    if date_index:
        lake_storage.index = lake_storage[date]
        
    lake_storage = lake_storage.sort(date)
    return lake_storage
    
    
def load_inflow_data(csv_name=None, date="Trading Date", parse_dates=True,
                     date_index=True, niwa_date=True):
    """
    Helper function to load the inflow data with optional initial data
    manipulation arguments
    
    Parameters
    ----------
    csv_name : Name of the csv file to be loaded, if None defaults to the
               repo file
    date : Name of the trading date column
    parse_dates : Boolean, whether to parse the trading dates
    date_index : Boolean, Should the date be assigned to an index
    
    Returns
    -------
    inflow_levels : DataFrame containing lake storage level information
    """
    
    if csv_name == None:
        csv_name = os.getcwd() + '/nzem/maps/Hydro_Inflow_Data.csv'
        
    inflow_levels = pd.read_csv(csv_name)
    if parse_dates:
        if niwa_date:
            inflow_levels[date] = inflow_levels[date].apply(parse_niwa_date)
        else:
            inflow_levels[date] = inflow_levels[date].apply(lambda x: parse(x))
            
    if date_index:
        inflow_levels.index = inflow_levels[date]
    
    inflow_levels = inflow_levels.sort(date)
    return inflow_levels
    
    
def relative_level(df, stat_col, ts_agg=None, ts_agg_name="",
                   agg=None, **kargs):
    """
    Perform a relative level calculation on the column in the dataframe
    This function automatically handles grouping and merging of the dataset
    
    Parameters
    ----------
    
    Returns
    -------
    """
    
    relative = nzem.utilities.ts_aggregation(df, stat_col, ts_agg=ts_agg, 
                                             agg=agg, **kargs)
    df["AC"] = df.index.map(ts_agg)
    dfnew = df.merge(pd.DataFrame({ts_agg_name: relative}), left_on="AC",
                  right_index=True)
    # Remove the temp agg function
    n = " ".join(["Relative", ts_agg_name])
    dfnew[n] = dfnew[stat_col] - dfnew[ts_agg_name]
    dfnew.sort_index()
    del dfnew["AC"]
    del dfnew[ts_agg_name]
    return dfnew

if __name__ == '__main__':
    pass
