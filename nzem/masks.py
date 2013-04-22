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
        
def mask(df, key, values, how=""):
    d = {"eq": df.eq_mask, "ne": df.ne_mask, "lt": df.lt_mask,
         "le": df.le_mask, "gt": df.gt_mask, "ge": df.ge_mask,
         "in", df.in_eqmask}
    return d[how](key, values)
    
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
    pd.DataFrame.mask = mask
    
    
    return pd.DataFrame
    
def seq_mask(s, value):
    return s[s == value]
    
def sgt_mask(s, value):
    return s[s > value]
    
def sge_mask(s, value):
    return s[s >= value]
    
def sle_mask(s, value):
    return s[s <= value]
    
def slt_mask(s, value):
    return s[s < value]
    
def sne_mask(s, value):
    return s[s != value]
    
def smask(s, value, how=""):
    d = {"ge": s.ge_mask, "le": s.le_mask, "eq": s.eq_mask, "lt": s.lt_mask,
         "gt": s.gt_mask, "ne": s.ne_mask}
    return d[how](value)
    
def apply_series_masks():
    pd.Series.eq_mask = seq_mask
    pd.Series.gt_mask = sgt_mask
    pd.Series.ge_mask = sge_mask
    pd.Series.lt_mask = slt_mask
    pd.Series.le_mask = sle_mask
    pd.Series.ne_mask = sne_mask
    pd.Series.mask = smask
    
    return pd.Series
        
if __name__ == '__main__':
        pass
