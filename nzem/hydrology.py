import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import glob
import nzem
from dateutil.parser import parse

def load_hydro_data(csv_name=None, parse_dates=True, date_index=True):
    """
    Load the 
    """    
    
    if csv_name == None:
        csv_name = os.getcwd() +  '/nzem/maps/Hydro_lake_Storage.csv'
    
    lake_storage = pd.read_csv(csv_name)
    if parse_dates:
        lake_storage["Trading Date"] = lake_storage["Trading Date"].apply(
                lambda x: parse(x))
    if date_index:
        lake_storage.index = lake_storage["Trading Date"]
        
    lake_storage = lake_storage.sort("Trading Date")
    return lake_storage
    


if __name__ == '__main__':
    pass
