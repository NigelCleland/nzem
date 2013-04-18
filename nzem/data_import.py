"""
Functionality to handle the importing and exporting of data 

Broadly contains the functionality to
a) Download csv data
b) Import csv data to a databse
c) Query a database for specific information
d) Import csv files with improved headers and mapping
e) Create a database from scratch

"""

# Module Imports
import pandas as pd
import psycopg2 as psy
import os
from datetime import datetime, timedelta
from dateutil.parser import parse
from pandas.tseries.offsets import Minute
import nzem


def execute_query(SQL, dbname="Electricity_Database", password="Hammertime",
    user="local_host", something=None):
    """
    Will connect to a database, execute a query and return a DataFrame
    containing the queried information
    
    Parameters
    ----------
    SQL : The SQL query to be executed 
    
    Returns
    -------
    df : A Pandas DataFrame containing the queried data
    """
    
    connect_string = "dbname='%(db)s' password='%(pw)s' user='%(ur)s' " % {
        'db': dbname, 'pw': password, 'ur': user}
    
    try:
        conn = psy.connect(connect_string)
        curs = conn.cursor()
    except:
        print "Unable to raise connection"
        
    curs.execute(SQL)
    headers = [desc[0] for desc in curs.description]
    data = curs.fetchall()
    return pd.DataFrame(data, columns=headers)

def load_map(map_fname=None):
    """ 
    Load a mapping DataFrame, will default to the Nodal Information
    Dataframe contained in the maps directory 
    
    Parameters
    ----------
    map_fname : Filename containing the path to the directory
    
    Returns
    -------
    df : A pandas dataframe containing the mapping information
    """
    if map_fname == None:
        map_fname = os.getcwd() +'/maps/Nodal_Information.csv'
        
    return pd.read_csv(map_fname)
    
def query_database(table, start_date=None, end_date=None, companies=None):
    """ Constructs an SQL query to query a particular table within the Database
        If no dates are specified will default to the current day
    """
    
    if start_date == None and end_date == None:
        date_format = "%d/%m/%Y"
        start_date = (datetime.now() - timedelta(days=1)).strftime(date_format)
        end_date = datetime.now().strftime(date_format)
        
    SQL = """
    SELECT * 
    FROM public.%(tab)s 
    WHERE %(tab)s."TRADING_DATE" BETWEEN "%(sd)s" AND "%(ed)s
    """
    
    if companies is not None:
        company_sql = """AND %(tab)s."COMPANY" in %(comp)s"""
        SQL = ''.join([SQL, company_sql])
        
    query_SQL = SQL % {'tab': table, 'sd': start_date, 'ed': end_date, 
                       'comp': companies}
                       
    print query_SQL
    
    
def map_data(df, node_map=None, left_on="Grid Exit Point", right_on="Node",
             map_reference="Island Name"):
    """
    Will map a DataFrame using the given Nodal Map and return the merged
    DataFrame
    
    Parameters
    ----------
    df : The DataFrame to be matched
    
    Returns
    -------
    merged_df : The df merged with the mapping dataframe
    """
             
    if node_map == None:
        node_map =load_map()
        
    partial_map = node_map[[right_on, map_reference]]
    return df.merge(partial_map, left_on=left_on, right_on=right_on) 
    
def load_reserve_prices(csv_name, tpid="Trading Period Id", date="Trading Date",
                        period="Trading Period", date_time="Date Time", 
                        date_time_index=False, title_columns=True):
    """
    Load reserve prices from a csv file and perform some basic data manipulations
    Assumes that the reserve prices have a unique trading period ID which should
    be manipulated
    
    """
    
    if tpid:
        res_prices = pd.read_csv(csv_name)
        res_prices[date] = res_prices[tpid].apply(lambda x: str(x)[:-2])
        res_prices[period] = res_prices[tpid].apply(lambda x: int(str(x)[-2:]))
        res_prices = res_prices[res_prices[period] <= 48]
        date_map = {x: parse(x) for x in res_prices[date].unique()}
        res_prices[date_time] = res_prices.apply(map_date_period, 
                            date_map=date_map, date=date, period=period, axis=1)
        res_prices = res_prices.sort(date_time)
        if date_time_index:
            res_prices.index = res_prices[date_time]
    else:
        res_prices = pd.read_csv(csv_name)
        
    if title_columns:
        col_dict = {x: x.replace('_', ' ').title() for x in res_prices.columns}
        res_prices = res_prices.rename(columns=col_dict)
        
    return res_prices
    
    
def map_date_period(series, date_map=None, date="Trading Date", 
                    period="Trading Period"):
                    
    pmap = lambda x: x * Minute(30)
    if date_map:
        return date_map[series[date]] + pmap(series[period])
    else:
        return parse(series[date]) + pmap(series[period])
        
        
def load_energy_prices(csv_name, date="Trading Date", period="Trading Period",
                       tpid="Trading Period Id", date_time="Date Time",
                       quick_parse=True, date_time_index=False, 
                       title_columns=True):
    """
    Load the energy prices from the standard five node file and apply some
    modifications to the dates in order to facilitate later merging.
    """
    
    en_prices = pd.read_csv(csv_name)
    
    if quick_parse:
        en_prices = en_prices.le_mask(period, 48)
        date_map = {x: parse(x) for x in en_prices[date].unique()}
        en_prices[date_time] = en_prices.apply(map_date_period,
                    date_map=date_map, date=date, period=period, axis=1)
        en_prices = en_prices.sort(date_time)
        if date_time_index:
            en_prices.index = en_prices[date_time]
            
    if title_columns:
        col_dict = {x: x.replace('_', ' ').title() for x in en_prices.columns}
        en_prices = en_prices.rename(columns=col_dict)
        
        
    return en_prices
    
def create_master_price_dataframe(energy_prices, reserve_prices):
    """
    Will create a master price dataframe from the energy and reserve price
    dataframes
    """
    
    # Split the reserve prices
    island_res_prices = reserve_prices.groupby(["Island Id", "Date Time"])["Price Sum"].sum()
    
    # Split the energy prices
    gr_en_prices = energy_prices.groupby(["Bus Id", "Date Time"])["Price Sum"].sum()
    
    # Begin merging to create a master data set...
    master_set = pd.DataFrame({"HAY2201 Price": gr_en_prices.ix["HAY2201"]})
    for bus in energy_prices["Bus Id"].unique():
        if bus != "HAY2201":
            new_name = "%s Price" % bus
            master_set = master_set.merge(pd.DataFrame({new_name: gr_en_prices.ix[bus]}), left_index=True,
                right_index=True)
    
    for island in ("NI", "SI"):
        new_name = "%s Reserve Price" % island
        master_set = master_set.merge(pd.DataFrame({new_name: 
            island_res_prices.ix[island]}), left_index=True, right_index=True)
            
    return master_set
    
def load_energy_offer_csv(csv_fname, date="TRADING_DATE", 
                          period="TRADING_PERIOD", date_time="Date Time",
                          quick_parse=True, date_time_index=False,
                          title_columns=True):
                          
    en_offers = pd.read_csv(csv_fname):
    if quick_parse:
        en_offers = en_offers.le_mask(period, 48)
        date_map = {x: parse(x) for x in en_offers[date].unique()}
        en_offers[date_time] = en_offers.apply(map_date_period, date_map=date_map,
            date=date, period=period)
        en_offers = en_offers.sort(date_time)
        
        if date_time_index:
            en_offers.index = en_offers[date_time]
            
    if title_columns:
        col_dict = {x: x.replace('_', ' ').title() for x in en_offers.columns}
        en_offers = en_offers.rename(columns=col_dict)
        
    return en_offers
    
    
    
if __name__ == '__main__':
    pass
