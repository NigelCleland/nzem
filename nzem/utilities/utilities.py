import pandas as pd
from datetime import datetime, timedelta

def niwa_parse(x, parse_str="%d%b%y"):
    d = datetime.strptime(x, parse_str)
    return d if d.year <= 2020 else datetime(d.year - 100, d.month, d.day)

if __name__ == '__main__':
    pass
    
    
    
    
