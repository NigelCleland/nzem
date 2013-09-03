"""
Interfance for interacting with the Gnash DB as set out by the Authority.

Based upon the work begun by David Hume in the EATools repository:
https://github.com/ElectricityAuthority/EAtools

Assumes you have the CDS extracted onto your computer
"""

# Standard Library
import os
import sys
import subprocess
import datetime as dt
from datetime import date, datetime, time, timedelta
from cStringIO import StringIO
import time

# No C Depencency
import simplejson as json
if sys.platform.startswith("linux"):
    from sh import Command

# C Depencencies
import pandas as pd
import numpy as np

# Need to get rid of these...
try:
    import pandas.io.sql as sql
    from pandas.tseries.offsets import Minute, Hour
except:
    print "Failed to import pandas modules, most likely in Read the Docs"

# Change to the gnash directory, assumes it is extracted in the user home path.

cwd = os.getcwd()

try:
    CONFIG = json.load(open(os.path.join(
        os.path.expanduser('~/python/nzem/nzem/_static'), 'config.json')))
    gnash_path = CONFIG['gnash-path']
except:
    print "CONFIG File does not exist"

# Set where your current Gnash directory is!



class Gnasher(object):
    """Gnasher is a class designed to make interfacing with the Gnash.exe as
    painless as possible. It should eventually handle constructing queries,
    returning data as pandas DataFrames and generally taking care of
    the BS which makes dealing with such systems "fun"
    """
    def __init__(self):
        super(Gnasher, self).__init__()
        try:
            os.chdir(gnash_path)
        except OSError:
            print "You may need to update your Gnash path for Gnasher to work"


    def query_energy(self, input_string):
        """
        Query the energy aspects of Gnash...
        Should test this to see if it works with multiple inputs
        """

        self._run_query(input_string)
        self._scrub_output()
        self.query = pd.read_csv(self.output, header=0, skiprows=[1])
        self._floss_DataFrame()
        return self.query


    def _run_query(self, input_string):
        # Trying to make the buffering process working.
        try:
            self.output = StringIO()
            def grab_output(line):
                self.output.write(line)
            self.gnash = Command("./Gnash.exe")
            self.gnash(_in=input_string, _out=grab_output).wait()
        except:
            print "Error, cannot run the query on Gnash"


    def _scrub_output(self):
        # Go line by line through the output until you find the header

        string = self.output.getvalue()
        self.output.close()

        begin = string.find("Aux.Date") - 1
        end = string.find("Gnash:Bye") - 2

        string = string[begin:end]

        self.output = StringIO(string)


    def _floss_DataFrame(self):
        """
        Floss (clean up) the DataFrame by completing a series of transformations.
        """

        # Convert values to floats
        for col in self.query.columns:
            if "Aux" not in col:

                self.query[col] = self.query[col].apply(self._object_converter)

        # Construct a datetime array
        self.query["DateTime"] = self.query.apply(self._datetime_converter, axis=1)

        # Rename the columns
        self.query.rename(columns={x: x.replace('.', '_') for x in self.query.columns},
                inplace=True)

        # Set the index
        self.query.set_index("DateTime", inplace=True)

        # Strip all nan values
        self.query = self.query.dropna()


    def _datetime_converter(self, series):
        """
        Convert to a DateTime object from date and period.
        Note, some of the periods are 1/2 periods, e.g. 4.5.
        These are currently returned as np.nan
        """

        try:
            date = datetime.strptime(series["Aux.Date"], "%d/%m/%Y")
            time = Minute(30) * int(series["Aux.HHn"]) - Minute(15)
        except:
            return np.nan

        return date + time


    def _object_converter(self, x):
        """
        Attempt to force the data types to be a bit more consistent!
        """
        if type(x) == str:
            return float(x.replace('\xc2\xb7', '.'))
        elif type(x) == int:
            return float(x)
        else:
            try:
                return float(x)
            except:
                return x

    def _get_names(self):
        """
        Place holder function, current plan is to grab all of the names off
        Gnash and then make these searchable.

        Can then have the user create "fuzzier" queries with the module
        constructing the rest.
        """

        pass

