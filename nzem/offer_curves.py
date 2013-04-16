"""
Construct Offer Curves for individual dataframes as needed.
"""

import pandas as pd
import numpy as np
import re

def stack_il_offer(df, rtype="6S"):
    """ 
    Stack a DataFrame by converting a multiple band offer to a stacked format
    
    Parameters
    ----------
    df : The DataFrame to be stacked, assumed to be in a raw format
    
    Returns
    -------
    stacked_df : The df which has been stacked and renamed
    """
    df = df.rename(columns={x: x.title().replace('_', ' ') for x in df.columns})
    gen = [x for x in df.columns if "Band" not in x]
    band_information = [x for x in df.columns if x not in gen]
    p = lambda x: '%s_Price' % rtype in x
    o = lambda x: '%s_Max' % rtype in x
    prices = filter(p, band_information)
    offers = filter(o, band_information)
    
    split_df = []
    for price, offer in itertools.izip(prices, offers):
        cols = gen + [price] + [offer]
        single_df = df[cols].copy()
        single_df = single_df.rename(columns={price: "Price", offer: "Offer"})
        single_df["Band"] = re.findall('[0-9]', price)[0]
        split_df.append(single_df)
        
    return pd.concat(split_df, ignore_index=true)
