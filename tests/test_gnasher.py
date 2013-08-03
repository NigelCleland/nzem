import nose
from nose import with_setup

import nzem.gnash.gnasher as gnasher
import datetime
import os
import pandas as pd

def test_setup():
	G = gnasher.Gnasher()
	assert G.gnash == "./Gnash.exe"

def test_ordinal_converter():
	G = gnasher.Gnasher()

	o1 = 38717.0104
	o2 = 39081.9896

	assert G._ordinal_converter(o1) == datetime.datetime(2006,1,1,0,15)
	assert G._ordinal_converter(o2) == datetime.datetime(2006, 12, 31, 23, 45)

def test_run_query():
	query = "dump ty for 2008 to ty.csv"
	output_file = "test_output.csv"

	G = gnasher.Gnasher()
	G._run_query(query, output_file)

	assert os.path.exists(output_file)
	os.remove(output_file)

def test_convertgnashdump():

	G = gnasher.Gnasher()
	G.gnash(_in="dump ty for 2008 to testconvert.csv", _out="testconvert.csv")

	M = G._convertgnashdump("testconvert.csv")	

	assert type(M) == pd.core.frame.DataFrame
	assert type(M.index) == pd.tseries.index.DatetimeIndex
	assert M.index.dtype == "<[M8[ns]"

def test_query_gnash():

	G = gnasher.Gnasher()

	df = G.query_gnash("dump ty for 2007 to test.csv", "test.csv", rm_file=True)

	assert type(df) == pd.core.frame.DataFrame
	assert not os.path.exists("test.csv")
	assert datetime.datetime(2007,4,8).strftime("%d/%m/%Y") in df["Aux_Date"]







