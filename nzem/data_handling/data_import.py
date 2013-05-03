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
#import psycopg2 as psy
import os
from datetime import datetime, timedelta
from dateutil.parser import parse
from pandas.tseries.offsets import Minute
import nzem
from nzem.utilities.utilities import niwa_parse
import glob

### Globals

NZEM_DATA_FOLDER = os.path.join(os.path.expanduser('~'), "data")


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
        map_fname = os.path.join(NZEM_DATA_FOLDER, 'maps/Nodal_Information.csv')
        
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
        node_map = load_map()
        
    partial_map = node_map[[right_on, map_reference]]
    return df.merge(partial_map, left_on=left_on, right_on=right_on) 
    
    
    
def map_date_period(series, date_map=None, date="Trading Date", 
                    period="Trading Period"):
    """
    Will map a trading date and a trading period together in order to form
    a single consistent date time parameter
    
    Parameters
    ----------
    series : A single series containing information about the row
    date_map : A dictionary containing mapping information regarding the 
        dates, is used to preparse a single copy of each date as the parse
        function tends to be very CPU intensive
    date : The date column name
    period : The period column name
    
    Returns
    -------
    Date Time : A single value of DataTime format containing the trading period
                and trading date data as a combined date and time.
    """
    
                    
    pmap = lambda x: x * Minute(30)
    if date_map:
        return date_map[series[date]] + pmap(series[period])
    else:
        return parse(series[date]) + pmap(series[period])
        
        
    
    
def load_csvfile(csv_name, quick_parse=True, date_time_index=True,
              title_columns=True, date_period=False, trading_period_id=False,
              date="Trading Date", period="Trading Period", 
              tpid="Trading Period Id", date_time="Date Time",
              niwa_date=False):
    """
    Master function to handle the importation of data files for analysis.
    Has capabilities of handling a broadish range of dates which is pretty sweet.
    All the is needed to run the file is a file and information about what the
    date column names are
    
    Parameters
    ----------
    csv_name : The name of the CSV file to be loaded
    quick_parse : Whether to apply a memoised version of the date time parsing
    date_time_index : Apply the date time column as the new index
    title_columns : Change column names to a "title" format
    date_period : Whether to apply the date_period parse method
    trading_period_id : Whether to first convert a TPID to date_period syntax
    date : The column name of the Trading Date
    period : Column name of the Trading Period
    tpid : Column name of the Trading Period ID
    date_time : Column name of the datetime index
    niwa_date : Whether the horrible NIWA date format is used (hydrology data..)
    
    Returns
    -------
    df : A sorted dataframe with the date time index loaded and working
    """      

    if os.path.exists(csv_name):
        df = pd.read_csv(csv_name)
    elif os.path.exists(os.path.join(NZEM_DATA_FOLDER, csv_name)):
        df = pd.read_csv(os.path.join(NZEM_DATA_FOLDER, csv_name))
    else:
        raise Exception("%s is not a valid file name" % csv_name)
        
    if quick_parse:
        if niwa_date:
            date_map = {x: niwa_parse(x) for x in df[date].unique()}
            df[date_time] = df[date].map(date_map)
        
        else:
            if trading_period_id:
                fdate = lambda x: str(x)[:-2]
                fperiod = lambda x: int(str(x)[-2:])
                df[date] = df[tpid].apply(fdate)
                df[period] = df[tpid].apply(fperiod)
                date_period = True
        
            if date_period:
                df = df.le_mask(period, 48)
                date_map = {x: parse(x) for x in df[date].unique()}
                df[date_time] = df.apply(map_date_period, date_map=date_map,
                    date=date, period=period, axis=1)
                
            else:
                raise Exception("Either date_period or trading_period_id must be \
                set to True")
            
    if date_time_index:
        df.index = df[date_time]
        
    if title_columns:
        col_dict = {x: x.replace('_', ' ').title() for x in df.columns}
        df = df.rename(columns=col_dict)
        
    return df.sort_index()
    
def create_il_dataset(folder_name="il_data", quick_parse=True, 
                      date_time_index=False, title_columns=True,
                      date_period=True, date="TRADING_DATE", 
                      period="TRADING_PERIOD", date_time="Date Time",
                      map_dataframe=True):
                      
    """ Load the full IL data set as a raw group of data """
    files = glob.glob(os.path.join(NZEM_DATA_FOLDER, folder_name, '*.csv'))
    
    dataframes = [load_csvfile(f, date_period=date_period, date=date, 
                               period=period, date_time_index=date_time_index,
                               date_time=date_time) 
                               for f in files]
                               
    df = pd.concat(dataframes, ignore_index=True)

    if map_dataframe:
        df = map_data(df)
    df.index = df[date_time]
    df = df.sort_index()
    return df
        
    
if __name__ == '__main__':
    pass
