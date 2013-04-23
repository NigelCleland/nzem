import pandas as pd
from datetime import datetime, timedelta

def niwa_parse(x, parse_str="%d%b%y"):
    d = datetime.strptime(x, parse_str)
    return d if d.year <= 2020 else datetime(d.year - 100, d.month, d.day)
    
def merge_series(df, series,**kargs):
    return df.merge(pd.DataFrame({series.name: series}), right_index=True, **kargs)

if __name__ == '__main__':
    pass
    
    
    
    
