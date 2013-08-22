""" Generic "Load All" function to make creating a merged dataset simpler
when beginning a new analysis

"""

import pandas as pd
import numpy as np
import nzem

def load_masterset():
    """ Load a master dataset will return a merged DataFrame containing the
    following information time alligned:
    
    1. Energy Prices (Five Nodes)
    2. Reserve Prices (Two Region, Two Type)
    3. Hydro Lake Storage
    4. Hydro Inflow Levels
    5. National Demand
    """
    
    # Apply the masks to the dataframes and series
    pd.DataFrame = nzem.apply_masks()
    pd.Series = nzem.apply_series_masks()
    
    # Load the five current datasets
    energy_prices = nzem.load_csvfile("Nodal_Pricing_Five_Node.csv", 
                                  date_period=True)
    reserve_prices = nzem.load_csvfile("reserve_prices.csv",
                                  trading_period_id=True)
    hydro_data = nzem.load_csvfile('hydro_data/Hydro_Lake_Data.csv',
                                   niwa_date=True)
    inflow_data = nzem.load_csvfile('hydro_data/Hydro_Inflow_Data.csv',
                                    niwa_date=True)
    demand_data = nzem.load_csvfile('island_demand_data.csv', date_period=True)
    
    # Columnise the price data
    all_res_prices = columnise_res_prices(reserve_prices)
    all_en_prices = columnise_energy_prices(energy_prices)
    
    # Resample the hydro data
    hd = hydro_data["Daily Stored"].resample('30Min', fill_method='ffill')
    ind = inflow_data["Daily Inflow"].resample('30Min', fill_method='ffill')
    
    # Nationalise the demand data
    nat_demand = demand_data.groupby(lambda x:x)["Demand Sum"].sum()
    nat_demand.name = "National Demand"
    
    # Merge all of the datasets
    
    md = all_en_prices.merge(all_res_prices, left_index=True, right_index=True)
    md = nzem.merge_dfseries(md, hd, left_index=True)
    md = nzem.merge_dfseries(md, ind, left_index=True)
    md = nzem.merge_dfseries(md, nat_demand, left_index=True)
    
    return md

def columnise_res_prices(df, island_price=True, islandid="Island Id",
    longname=False):
    """ 
    Columnise the reserve price dataframe
    """
    
    if longname:
        ni = "North Island"
        si = "South Island"
    else:
        ni = "NI"
        si = "SI"
    
    # Split them
    nif = df.eq_mask("Reserve Type", "F").eq_mask(islandid, ni)
    nis = df.eq_mask("Reserve Type", "S").eq_mask(islandid, ni)
    sif = df.eq_mask("Reserve Type", "F").eq_mask(islandid, si)
    sis = df.eq_mask("Reserve Type", "S").eq_mask(islandid, si)
    
    # Rename columns
    nif = nif.rename(columns={"Price Sum": "NI FIR Price"})
    nis = nis.rename(columns={"Price Sum": "NI SIR Price"})
    sif = sif.rename(columns={"Price Sum": "SI FIR Price"})
    sis = sis.rename(columns={"Price Sum": "SI SIR Price"})
    
    # Remerge them back together
    
    mdf = nzem.merge_series(nif["NI FIR Price"], nis["NI SIR Price"])
    mdf = nzem.merge_dfseries(mdf, sif["SI FIR Price"], left_index=True)
    mdf = nzem.merge_dfseries(mdf, sis["SI SIR Price"], left_index=True)
    
    if island_price:
        mdf["NI Reserve Price"] = mdf["NI FIR Price"] + mdf["NI SIR Price"]
        mdf["SI Reserve Price"] = mdf["SI FIR Price"] + mdf["SI SIR Price"]
    
    return mdf
    
def columnise_energy_prices(df, island_split=True):
    """
    Columnise the energy price dataframe
    """
    
    # Separate the columns
    hay = df.eq_mask("Bus Id", "HAY2201")
    ben = df.eq_mask("Bus Id", "BEN2201")
    sfd = df.eq_mask("Bus Id", "SFD2201")
    hly = df.eq_mask("Bus Id", "HLY2201")
    ota = df.eq_mask("Bus Id", "OTA2201")
    
    # Rename the columns
    hay = hay.rename(columns={"Price Sum": "HAY2201 Price"})
    ben = ben.rename(columns={"Price Sum": "BEN2201 Price"})
    sfd = sfd.rename(columns={"Price Sum": "SFD2201 Price"})
    hly = hly.rename(columns={"Price Sum": "HLY2201 Price"})
    ota = ota.rename(columns={"Price Sum": "OTA2201 Price"})
    
    # Remerge them
    mdf = nzem.merge_series(hay["HAY2201 Price"], ben["BEN2201 Price"])
    mdf = nzem.merge_dfseries(mdf, sfd["SFD2201 Price"], left_index=True)
    mdf = nzem.merge_dfseries(mdf, hly["HLY2201 Price"], left_index=True)
    mdf = nzem.merge_dfseries(mdf, ota["OTA2201 Price"], left_index=True)
    
    if island_split:
        mdf["Island Price Split"] = mdf["HAY2201 Price"] - mdf["BEN2201 Price"]
        
    return mdf
