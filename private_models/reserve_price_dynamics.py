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
    del il_dataset
    il_fir.index = il_fir["Date Time"]
    allre = il_fir.merge(allr_prices, left_index=True, right_index=True)
    del il_fir
    #il_sir = nzem.stack_il_offer(il_dataset, rtype="60S")
    
    nidata = allre.eq_mask("Island Name", "North Island")
    del allre
    dispatch = nidata[nidata["Price"] <= nidata["NI FIR Price"]]
    del nidata
    grouped = dispatch.groupby(["Date Time", "Company"])["Offer"].sum()
    del dispatch
    
    arr = np.array(grouped.index.tolist())
    df = pd.DataFrame({"Dispatched Offer": grouped})
    df["Date Time"] = arr[:,0]
    df["Company"] = arr[:,1]
    df.index = df["Date Time"]
    dfmerg = nzem.merge_dfseries(df, allr_prices["NI FIR Price"], left_index=True)
    
    dfmerg["Year"] = dfmerg["Date Time"].apply(lambda x: x.year)
    dfmerg["Month"] = dfmerg["Date Time"].apply(lambda x: x.month)
    dfmerg["TP"] = dfmerg["Date Time"].apply(nzem.tp_tsagg)
    
    cavg = dfmerg.groupby(["Company", "Year", "Month", "TP"])
    cavg = cavg["Dispatched Offer"].aggregate(np.max)
    cavg.name = "75% Offer"
    dfall = nzem.merge_dfseries(dfmerg, cavg, left_on=["Company", "Year", "Month", "TP"])
    
    def relative_offer(series):
        return 1. * series["Dispatched Offer"] / series["75% Offer"] if series["75% Offer"] > 0 else 0 if series["Dispatched Offer"] == 0 else 1
        
    dfall["Relative Offer"] = dfall.apply(relative_offer, axis=1)
    dfall["Offer Weighted Price"] = dfall["Relative Offer"] * dfall["NI FIR Price"]
    
    def get_ratio(df, comp):
    
        t1 = df.eq_mask("Company", comp).groupby(["Year", "Month"])["NI FIR Price"].sum() 
        t2 = df.eq_mask("Company", comp).groupby(["Year", "Month"])["Offer Weighted Price"].sum() 
        
        m = (t2 / t1)
        m = pd.DataFrame({"Ratio" : m})
        arr = np.array(m.index.tolist())
        m["Year"] = arr[:,0]
        m["Month"] = arr[:,1]
        m["Price"] = t1
        def d(x):
            return datetime.date(x[0], x[1],1)
            
        m["Date"] = m[["Year", "Month"]].apply(d, axis=1)
        m.index = m["Date"]
        
        return m[["Ratio", "Price"]]
    
