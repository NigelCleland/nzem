import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import glob
import nzem
from dateutil.parser import parse
from datetime import datetime, timedelta  
from nzem.utilities.utilities import *
from nzem.utilities.ts_aggregation import *
from nzem.utilities.aggregation import *
    
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
    
    relative = ts_aggregation(df, stat_col, ts_agg=ts_agg, 
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
