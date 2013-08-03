"""
Interfance for interacting with the Gnash DB as set out by the Authority.

Based upon the work begun by David Hume in the EATools repository:
https://github.com/ElectricityAuthority/EAtools

Assumes you have the CDS extracted onto your computer
"""

import pandas as pd
import numpy as np
from datetime import date, datetime, time, timedelta
import pandas.io.sql as sql
#import pyodbc
import os
import sys
import subprocess
from cStringIO import StringIO

if sys.platform.startswith("linux"):
   from pbs import Command

# Change to the gnash directory, assumes it is extracted in the user home path.

cwd = os.getcwd()

gnash_path = os.path.join(os.path.expanduser('~'), 'CDS', 'CentralisedDataset', 'HalfHourly')
os.chdir(gnash_path)



class Gnasher(object):
    """Gnasher is a class designed to make interfacing with the Gnash.exe as 
    painless as possible. It should eventually handle constructing queries,
    returning data as pandas DataFrames and generally taking care of
    the BS which makes dealing with such systems "fun"
    """
    def __init__(self):
        super(Gnasher, self).__init__()
        self._initialise_connection()

    def query_gnash(self, input_string, output_file=None):
    
        
        self._run_query(input_string, output_file=output_file)
        self.query = self._convertgnashdump(output_file)
        return self.query


    def _initialise_connection(self):
        try:
            self.gnash = Command("./Gnash.exe")
        except:
            print "Error, cannot create a connection to Gnash"

    def _run_query(self, input_string, output_file=None):
        try:
            return self.gnash(_in=input_string, _out=output_file)
        except:
            print "Error, cannot run the query on Gnash"

    
    def _convertgnashdump(self, output_file):

        na_conv = lambda x: np.nan if "?" in str(x) else x

        # First read to obtain dump file
        Gin=pd.read_csv(output_file, header=1, skiprows=[2])
        # Dictionary Comprehension to rename columns
        new_names = {x: x.replace('.', '_') for x in Gin.columns}
        Gin.rename(columns=new_names, inplace=True)
        Gin = Gin.applymap(na_conv)
        # Set a new index
        Gin["Aux_DayClock"] = Gin["Aux_DayClock"].apply(self._ordinal_converter)
        Gin = Gin.set_index('Aux_DayClock')
        return Gin

    def _ordinal_converter(self, x):
        if np.isnan(x):
            return np.nan
    
        x =float(x) + datetime.toordinal(datetime(1899,12,31))
        ord_date = date.fromordinal(int(np.floor(x)))
         
        return datetime(ord_date.year,
                        ord_date.month,
                        ord_date.day,
                        int(np.floor((x % 1.0) * 24)),
                        int(np.round((x % 1.0 * 24 % 1.0 * 60), decimals=0)))
            