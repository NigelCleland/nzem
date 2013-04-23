import pandas as pd
from datetime import datetime, timedelta

def niwa_parse(x, parse_str="%d%b%y"):
    d = datetime.strptime(x, parse_str)
    return d if d.year <= 2020 else datetime(d.year - 100, d.month, d.day)
    
def merge_dfseries(df, series,**kargs):
    return df.merge(pd.DataFrame({series.name: series}), 
                    right_index=True, **kargs)
                    
def merge_series(s1, s2):
    return pd.DataFrame({s1.name: s1}).merge(pd.DataFrame({s2.name: s2}),
        left_index=True, right_index=True)

if __name__ == '__main__':
    pass
    
    
    
    
