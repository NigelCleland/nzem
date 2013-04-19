"""
Add custom functionality to the Pandas DataFrame module to allow for 
easier splitting and indexing of data
"""

import pandas as pd
import numpy as np
from itertools import izip

def eq_mask(df, key, value):
    return df[df[key] == value]
    
def ge_mask(df, key, value):
    return df[df[key] >= value]
    
def gt_mask(df, key, value):
    return df[df[key] > value]
    
def le_mask(df, key, value):
    return df[df[key] <= value]
    
def lt_mask(df, key, value):
    return df[df[key] < value]
    
def ne_mask(df, key, value):
    return df[df[key] != value]
    
def gen_mask(df, f):
    return df[f(df)]
    
def in_eqmask(df, key, values):
    l = (df.eq_mask(key, value) for value in values)
    return pd.concat(l).drop_duplicates()
    
def mix_eqmask(df, keys, values):
    l = (df.eq_mask(key, value) for key, value in izip(keys, values))
    return pd.concat(l).drop_duplicates()
    
def mixbool_mask(df, keys, bools, values):
    bool_dict = {"eq": df.eq_mask, "ge": df.ge_mask, "le": df.le_mask,
                 "lt": df.lt_mask, "gt": df.gt_mask, "ne": df.ne_mask}
    l = (bool_dict[b](key, value) for b, key, value in izip(bools, keys, values))
    return pd.concat(l).drop_duplicates()
    
def bet_mask(df, key, values, inclusive=True):
    if inclusive:
        return df.ge_mask(key, values[0]).le_mask(key, values[1])
    else:
        return df.gt_mask(key, values[0]).lt_mask(key, values[1])
    
def apply_masks():
    
    pd.DataFrame.eq_mask = eq_mask
    pd.DataFrame.ge_mask = ge_mask
    pd.DataFrame.gt_mask = gt_mask
    pd.DataFrame.le_mask = le_mask
    pd.DataFrame.lt_mask = lt_mask
    pd.DataFrame.ne_mask = ne_mask
    pd.DataFrame.gen_mask = gen_mask
    pd.DataFrame.in_eqmask = in_eqmask
    pd.DataFrame.mix_eqmask = mix_eqmask
    pd.DataFrame.mixbool_mask = mixbool_mask
    pd.DataFrame.bet_mask = bet_mask
    
    return pd.DataFrame
        
    if __name__ == '__main__':
        pass
