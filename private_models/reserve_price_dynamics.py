import pandas as pd
import numpy as np
import nzem

def year_per_creation(series, years, percentages, agg=None):
    array = np.zeros((len(years), len(percentages)))
    for i, year in enumerate(years):
        for j, percent in enumerate(percentages):
            val = nzem.point_reduced_aggregation(series.ix[str(year)], percent, agg=agg)
            array[i, j] = val
            
    return pd.DataFrame(array, index=years, columns=percentages)



if __name__ == '__main__':
    
    res_prices = nzem.load_csvfile("reserve_prices.csv", trading_period_id=True)
    allr_prices = nzem.columnise_res_prices(res_prices)
    
    # Will need to carefully talk about the years and dates as well
    # Include reasoning for doing sum vs mean etc
    
    nif = nzem.reduced_aggregation(allr_prices["NI FIR Price"], agg=np.sum, percent=True, npoints=1000)
    nis = nzem.reduced_aggregation(allr_prices["NI SIR Price"], agg=np.sum, percent=True, npoints=1000)
    sif = nzem.reduced_aggregation(allr_prices["SI FIR Price"], agg=np.sum, percent=True, npoints=1000)
    sis = nzem.reduced_aggregation(allr_prices["SI SIR Price"], agg=np.sum, percent=True, npoints=1000)
    nia = nzem.reduced_aggregation(allr_prices["NI Reserve Price"], agg=np.sum, percent=True, npoints=1000)
    sia = nzem.reduced_aggregation(allr_prices["SI Reserve Price"], agg=np.sum, percent=True, npoints=1000)
    
    # Begin differentiating...
    
    years = np.arange(2008, 2013, 1)
    percentages = (0., 1., 2.5, 5.)
    
    nifirdf = year_per_creation(allr_prices["NI FIR Price"], years, percentages, agg=np.sum)
    nisirdf = year_per_creation(allr_prices["NI SIR Price"], years, percentages, agg=np.sum)
    sifirdf = year_per_creation(allr_prices["SI FIR Price"], years, percentages, agg=np.sum)
    sisirdf = year_per_creation(allr_prices["SI SIR Price"], years, percentages, agg=np.sum)
            
    coldict = {0: "Full Available [$/MW]", 1: "1% Unavailable [$/MW]", 2.5: "2.5% Unavailable [$/MW]", 5: "5% Unavailable [$/MW]"}


    
    nifirdf = nifirdf.rename(columns=coldict)
    sifirdf = sifirdf.rename(columns=coldict)
    nisirdf = nisirdf.rename(columns=coldict)
    sisirdf = sisirdf.rename(columns=coldict)

    
    # Plots to include
    # nia, sia, nifirdf, nisirdf, sifirdf, sisirdf
    # Concludes section one, price dynamics
    # Need to move onto the individual company "effectiveness" factors next
    
    # Need to load me some IL data yo!
    
    il_dataset = nzem.create_il_dataset()
    il_fir = nzem.stack_il_offer(il_dataset, rtype="6S")
    il_sir = nzem.stack_il_offer(il_dataset, rtype="60S")
