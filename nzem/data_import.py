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
             map_reference="Island Name")
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
    
if __name__ == '__main__':
    pass
