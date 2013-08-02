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
import pyodbc
import os
import sys

if sys.platform.startswith("linux"):
   from pbs import Command

class Gnasher(object):
	"""Gnasher is a class designed to make interfacing with the Gnash.exe as 
	painless as possible. It should eventually handle constructing queries,
	returning data as pandas DataFrames and generally taking care of
	the BS which makes dealing with such systems "fun"
	"""
	def __init__(self, arg):
		super(Gnasher, self).__init__()
		self.arg = arg
		self._initialise_connection()

	def query_gnash(input_string):
		output_file = "temporary_file.csv"
		self._run_query(input_string, output_file)
		self.query = self._convertgnashdump(output_file)
		return self.query


	def _initialise_connection():
		try:
			self.gnash = Command("./Gnash.exe")
		except:
			print "Error, cannot create a connection to Gnash"

	def _run_query(input_string, output_file):
		try:
			self.gnash(_in=input_string, _out=output_file)
		except:
			print "Error, cannot run the query on Gnash"

	
	def _convertgnashdump(output_file):

		# First read to obtain dump file
   		Gin=pd.read_csv(output_file, header=1, skiprows=[2], 
   			na_values=['?','       ? ','       ?','          ? ','          ?','        ?'],
   			converters={'Aux.DayClock': ordinal_converter}, parse_dates=True)
   		names = Gin.columns #get names used by Gnash
   		# Dictionary Comprehension to rename columns
   		new_names = {x: x.replace('.', '_')}
   		Gin.rename(columns=new_names, inplace=True)
   		# Set a new index
		Gin = Gin.set_index('Aux_DayClock')
   		return Gin

	def _ordinal_converter(x):
		x=float(x) + datetime.toordinal(datetime(1899,12,31))
		
		return datetime(date.fromordinal(int(np.floor(x))).year,
						date.fromordinal(int(np.floor(x))).month,
						date.fromordinal(int(np.floor(x))).day,
						int(np.floor(float("{0:.2f}".format((x%1.0)*24)))),
						int((float("{0:.2f}".format((x%1.0)*24))%1.0)*60.0))
			