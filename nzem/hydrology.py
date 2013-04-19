import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import glob
import nzem

def load_hydro_data(csv_name=None, parse_dates=1):
    """
    Load the 
    """    
    
    if csv_name == None:
        csv_name = './maps/Hydro_lake_Storage.csv'
    
    lake_storage = pd.read_csv(csv_name, parse_dates=parse_dates, dayfirst=True)
    return lake_storage
    


if __name__ == '__main__':
    pass
