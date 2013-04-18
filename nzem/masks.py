"""
Add custom functionality to the Pandas DataFrame module to allow for 
easier splitting and indexing of data
"""

import pandas as pd

def equal_mask(df, key, value):
    return df[df[key] == value]
    
def ge_mask(df, key, value):
    return df[df[key] >= value]
    
def gt_mask(df, key, value):
    return df[df[key] > value]
    
def le_mask(df, key, value):
    return df[df[key] <= value]
    
def lt_mask(df, key, value):
    return df[df[key] < value]
    
def noteq_mask(df, key, value):
    return df[df[key] != value]
    
def gen_mask(df, f):
    return df[f(df)]
    
def apply_masks():
    
    pd.DataFrame.equal_mask = equal_mask
    pd.DataFrame.ge_mask = ge_mask
    pd.DataFrame.gt_mask = gt_mask
    pd.DataFrame.le_mask = le_mask
    pd.DataFrame.lt_mask = lt_mask
    pd.DataFrame.noteq_mask = noteq_mask
    pd.DataFrame.gen_mask = gen_mask
    
    return pd.DataFrame
    
if __name__ == '__main__':
    pass
