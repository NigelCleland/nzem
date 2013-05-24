import pandas as pd
import nzem
import numpy as np
import matplotlib.pyplot as plt
import matplotlib

font = {'family': 'serif', 'size': 18}
matplotlib.rc('font', **font)


def part_match(x, colset):
    for item in colset:
        if item in x:
            return True
    
    return False
    
def construct_cond_prob(ms1, ms2, seasons, periods, how='ge'):
    ni_v = []
    for s in seasons:
        for p in periods:
            t1 = ms1.eq_mask("Season Name", s).eq_mask("Time Name", p)["Relative Level"]
            t2 = ms2.eq_mask("Season Name", s).eq_mask("Time Name", p)["Relative Level"]
            prob = nzem.conditional_probability(t1, t2, step=20, how=how)
            prob = pd.DataFrame({"Probability": prob})
            prob["Season"] = s
            prob["Time"] = p
            prob["Lake Level"] = prob.index
            prob.index = np.arange(len(prob))
            ni_v.append(prob.copy())
              
    all_prob = pd.concat(ni_v, ignore_index=True)
    all_prob = all_prob.dropna()
    all_prob.index = all_prob["Lake Level"]
    return all_prob
    
def weighted_model(ms1, ms2, seasons, periods, step=20):
    ni_v = []
    for s in seasons:
        for p in periods:
            t1 = ms1.eq_mask("Season Name", s).eq_mask("Time Name", p)["Relative Level"]
            t2 = ms2.eq_mask("Season Name", s).eq_mask("Time Name", p)["Relative Level"]

            p1 = nzem.conditional_probability(t1, t2, step=step, how='ge')
            p2 = nzem.conditional_probability(t1, t2, step=step, how='le')
            s1 = nzem.conditional_sample_weight(t2, step=step, how='ge')
            s2 = nzem.conditional_sample_weight(t2, step=step, how='le')
            pnew = p1 * s1 + p2 * s2
            df = pd.DataFrame({"Weighted Probability": pnew})
            df["Season"] = s
            df["Time"] = p
            df["Lake Level"] = df.index
            df.index = np.arange(len(df))
            ni_v.append(df.copy())

    allprob = pd.concat(ni_v, ignore_index=True)
    allprob = allprob.dropna()
    allprob.index = allprob["Lake Level"]
    return allprob
    
def forecast_plot(isl):
    
    fig, ax1 = plt.subplots(1, 1, figsize=(16,9))
    ax1.plot(isl["Model Prediction"], 'k-', label="Model Prediction")
    ax1.plot(isl["Actual"], 'kx', label="Actual Number")
    ax1.plot(isl["Difference"], 'k--', label="Model Difference")
    ax1.set_xlabel("Minimum Model Probability [%]")
    ax1.set_ylabel("Number of Periods")
    
    #ax2 = ax1.twinx()
    #ax2.plot(isl["Percentage Difference"], 'k--', label="Percentage Difference")
    #ax2.set_ylabel("Percentage Difference")
    ax1.legend()
    #ax2.legend()
    return fig

def get_probability(series, mod=None):
    try:
        n = mod.eq_Season("mask", series["Season Name"])
        # Iteratively reduce the sample
        n = n.eq_mask("Time", series["Time Name"])
        n = n.ge_mask("Lake Level", series["Relative Level"])
        n = n.ix[0]["Weighted Probability"]
    except IndexError:
        n = np.nan
    return n
    
def prob_model(ge, le, nimod):
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    
    ge.eq_mask("Season", "Summer").eq_mask("Time", "Peak")["Probability"].plot(
            ax=axes, style='k.', label="Greater Than")

    le.eq_mask("Season", "Summer").eq_mask("Time", "Peak")["Probability"].plot(
            ax=axes, style='kx', label="Less Than")
    nimod.eq_mask("Season", "Summer").eq_mask("Time", "Peak")["Weighted Probability"].plot(
            ax=axes, style='k-', label="Weighted Model")    
    
    axes.set_xlabel("Relative National Hydro Level [GWh]")
    axes.set_ylabel("Probability of a Constraint Occurring")
    axes.grid(axis='y')
    axes.legend(loc='best')
    return fig, axes


if __name__ == '__main__':

    pd.DataFrame = nzem.apply_masks()
    pd.Series = nzem.apply_series_masks()
    
    # Load the master dataset
    masterset = nzem.load_masterset()
    
    # Raw Hydro data for relative levels
    lake_data = nzem.load_csvfile("hydro_data/Hydro_Lake_Data.csv", niwa_date=True)
    inflow_data = nzem.load_csvfile("hydro_data/Hydro_Inflow_Data.csv", niwa_date=True)
    
    # Get the lower decile for inflows and lake_data
    decile_hydro_level = nzem.ts_aggregation(lake_data, "Daily Stored",
            ts_agg=nzem.doy_tsagg, agg=nzem.per10)
    decile_hydro_level.name = "Decile Storage"
    masterset["DOY"] = masterset.index.map(nzem.doy_tsagg)
    masterset = nzem.merge_dfseries(masterset, decile_hydro_level, left_on="DOY")
    masterset["Relative Level"] = masterset["Daily Stored"] - masterset["Decile Storage"]
    
    
    # Load the NFR data:
    nfr_data = nzem.load_csvfile("All_NFR_Data.csv", tpid="TRADING_PERIOD_ID",
                                 trading_period_id=True)
                                 
    # Get hydro data
    hvdc_data = nfr_data.eq_mask("Risk Six Sec Plant", "HVDC").eq_mask("Risk Sixty Sec Plant", "HVDC")
    
    col_set = ["Nfr", "Reserve", "Risk", "Mw"]            
    cols = [x for x in hvdc_data.columns if part_match(x, col_set)]
#    ghvdc_data = hvdc_data.groupby(["Island Name", "Date Time"])[cols].mean()
    ghvdc_data = nfr_data.groupby(["Island Name", "Date Time"])[cols].max()
    
    # Determine which periods are constrained
    
    masterset["NI Constraint"] = masterset.apply(nzem.HVDC_Constraint, 
            energy_price_send="BEN2201 Price", energy_price_receive="HAY2201 Price",
            res_price="NI Reserve Price", abs_tol=5, axis=1)
            
    masterset["SI Constraint"] = masterset.apply(nzem.HVDC_Constraint, 
            energy_price_send="HAY2201 Price", energy_price_receive="BEN2201 Price",
            res_price="SI Reserve Price", abs_tol=5, axis=1)
            
    # Apply some simple factors
    masterset["Trading Period"] = masterset.index.map(nzem.tp_tsagg)
    masterset["Month"] = masterset.index.map(nzem.moy_tsagg)
    masterset["Season"] = masterset.index.map(nzem.season_tsagg)
    masterset["WOY"] = masterset.index.map(nzem.woy_tsagg)
    
    # Begin determining the conditional probabilities for each factor:
    masterset["Year"] = masterset.index.map(lambda x: x.year)
    masterset_old = masterset.copy()
    masterset = masterset.le_mask("Year", 2012)

    
    #### Going to need to begin the probability model again... SIGH ####
    
    # Create different times
    offpeak = np.arange(1, 15, 1)
    shoulder1 = np.arange(15, 18, 1)
    peak = np.arange(18, 38, 1)
    shoulder2 = np.arange(38, 44)
    offpeak2 = np.arange(44, 51)
    
    period_map = {}
    for o in offpeak:
        period_map[o] = "Offpeak"
    for s in shoulder1:
        period_map[s] = "Shoulder"
    for p in peak:
        period_map[p] = "Peak"
    for s in shoulder2:
        period_map[s] = "Shoulder"
    for o in offpeak2:
        period_map[o] = "Offpeak"

    season_map = {1: "Summer", 2: "Autumn", 3: "Winter", 4: "Spring"}        
    masterset["Season Name"] = masterset["Season"].map(season_map)
    masterset["Time Name"] = masterset["Trading Period"].map(period_map)
    masterset.sort_index()    
    ni_masterset = masterset.eq_mask("NI Constraint", True)
    si_masterset = masterset.eq_mask("SI Constraint", True) 
    
    ses = ["Summer", "Autumn", "Winter", "Spring"]
    per = ["Offpeak", "Shoulder", "Peak"]

    # Note, these two commands will construct the conditional probability
    # For each of the seasons and periods which have been applied to the data
    # I'm     
    nige_probs = construct_cond_prob(ni_masterset, masterset, ses, per, how='ge')
    nile_probs = construct_cond_prob(ni_masterset, masterset, ses, per, how='le')
    sile_probs = construct_cond_prob(si_masterset, masterset, ses, per, how='le')
    sige_probs = construct_cond_prob(si_masterset, masterset, ses, per, how='ge')
    
    
    # Create the weighted models
    nimod = weighted_model(ni_masterset, masterset, ses, per, step=10)
    simod = weighted_model(si_masterset, masterset, ses, per, step=10)


        
    masterset["NI Model Prediction"] = masterset.apply(get_probability, mod=nimod, axis=1)
    masterset["SI Model Prediction"] = masterset.apply(get_probability, mod=simod, axis=1)
    
    n = np.arange(0, 1, 0.01)
    ni = []
    si = []
    
    for i in n:
        ni.append([
        masterset.ge_mask("NI Model Prediction", i)["NI Model Prediction"].sum(),
        masterset.ge_mask("NI Model Prediction", i).eq_mask("NI Constraint", True)["DOY"].count()])
        
        si.append([
        masterset.ge_mask("SI Model Prediction", i)["SI Model Prediction"].sum(),
        masterset.ge_mask("SI Model Prediction", i).eq_mask("SI Constraint", True)["DOY"].count()])
        
    ni = pd.DataFrame(ni, index=n, columns=["Model Prediction", "Actual"])
    si = pd.DataFrame(si, index=n, columns=["Model Prediction", "Actual"])
    
    ni["Percentage Difference"] = 100. * ni["Model Prediction"] / ni["Actual"] - 100
    si["Percentage Difference"] = 100. * si["Model Prediction"] / si["Actual"] - 100
    
    ni["Difference"] = ni["Model Prediction"] - ni["Actual"]
    si["Difference"] = si["Model Prediction"] - si["Actual"]
    
    ni[["Model Prediction", "Actual", "Difference"]].plot(secondary_y="Difference")
    si[["Model Prediction", "Actual", "Difference"]].plot(secondary_y="Difference")
    
    ni[["Model Prediction", "Actual", "Percentage Difference"]].plot(secondary_y="Percentage Difference")
    si[["Model Prediction", "Actual", "Percentage Difference"]].plot(secondary_y="Percentage Difference")
    
    scale = 0.82
    ni_scale = []
    for i in n:
        ni_scale.append([
        (masterset.ge_mask("NI Model Prediction", i)["NI Model Prediction"]*scale).sum(),
        masterset.ge_mask("NI Model Prediction", i).eq_mask("NI Constraint", True)["DOY"].count()])
    ni_scale = pd.DataFrame(ni_scale, index=n, columns=["Model Prediction", "Actual"])
    
    ni_scale["Percentage Difference"] = 100. * ni_scale["Model Prediction"] / ni_scale["Actual"] - 100  
    ni_scale["Difference"] = ni_scale["Model Prediction"] - ni_scale["Actual"]
    
    def prob_assesment(df, col, col_const):
        sc = []
        n = np.arange(0, 1, 0.01)
        for i in n:
            sc.append([
                df.ge_mask(col, i)[col].sum(),
                df.ge_mask(col, i).eq_mask(col_const, True)["DOY"].count()])
        
        scale = pd.DataFrame(sc, index=n, columns=["Model Prediction", "Actual"])
        scale["Percentage Difference"] = 100. * scale["Model Prediction"] / scale["Actual"] - 100
        scale["Difference"] = scale["Model Prediction"] - scale["Actual"]
        return scale
        
                
    
    #scale = 0.82
    #ni_scale = []
    #for i in n:
    #    ni_scale.append([
    #   (masterset.le_mask("NI Model Prediction", i)["NI Model Prediction"]*scale).sum(),
    #    masterset.le_mask("NI Model Prediction", i).eq_mask("NI Constraint", True)["DOY"].count()])
    #ni_scale = pd.DataFrame(ni_scale, index=n, columns=["Model Prediction", "Actual"])
    
    #ni_scale["Percentage Difference"] = 100. * ni_scale["Model Prediction"] / ni_scale["Actual"] - 100  
    #ni_scale["Difference"] = ni_scale["Model Prediction"] - ni_scale["Actual"]
    
    scale = 0.9
    si_scale = []
    for i in n:
        si_scale.append([
        (masterset.ge_mask("SI Model Prediction", i)["SI Model Prediction"]*scale).sum(),
        masterset.ge_mask("SI Model Prediction", i).eq_mask("SI Constraint", True)["DOY"].count()])
    si_scale = pd.DataFrame(si_scale, index=n, columns=["Model Prediction", "Actual"])
    
    si_scale["Percentage Difference"] = 100. * si_scale["Model Prediction"] / si_scale["Actual"] - 100  
    si_scale["Difference"] = si_scale["Model Prediction"] - si_scale["Actual"]
    

        
    fig, axes = prob_model(nige_probs, nile_probs, nimod)
    fig.savefig("prob_models.png", dpi=150)  
        
    fig = forecast_plot(ni)
    fig.savefig("NI_init_model.png", dpi=150)
    fig = forecast_plot(si)
    fig.savefig("SI_init_model.png", dpi=150)
    
    fig = forecast_plot(ni_scale)
    fig.savefig("NI_Correct.png", dpi=150)
    fig = forecast_plot(si_scale)
    fig.savefig("SI_Correct.png", dpi=150)
        
        
    # Load all of the new data
    reprice = nzem.load_csvfile("Model_ds/MarAprResPrices.csv", trading_period_id=True)
    enprice = nzem.load_csvfile("Model_ds/MarchAprilEnergyPrices.csv", date_period=True)
    lakes = nzem.load_csvfile("Model_ds/MarAprlHydrodata.csv", niwa_date=True)
    
    # Create a single dataset from the new data
    hay = enprice.eq_mask("Bus Id", "HAY2201")
    ben = enprice.eq_mask("Bus Id", "BEN2201")
    rp = nzem.columnise_res_prices(reprice, islandid="Island Name", longname=True)
    
    hay = hay.rename(columns={"Price Sum": "HAY2201 Price"})
    ben = ben.rename(columns={"Price Sum": "BEN2201 Price"})

    mdf = nzem.merge_series(hay["HAY2201 Price"], ben["BEN2201 Price"])
    mdf = pd.merge(mdf, rp, left_index=True, right_index=True)
    
    lakesh = lakes["Daily Stored"].resample("30Min", fill_method='ffill')
    mdf = nzem.merge_dfseries(mdf, lakesh, left_index=True)
    
    mdf["NI Constraint"] = mdf.apply(nzem.HVDC_Constraint,
        energy_price_send="BEN2201 Price", energy_price_receive="HAY2201 Price", 
        res_price="NI Reserve Price", abs_tol=5, axis=1)
        
    mdf["SI Constraint"] = mdf.apply(nzem.HVDC_Constraint,
        energy_price_receive="BEN2201 Price", energy_price_send="HAY2201 Price", 
        res_price="SI Reserve Price", abs_tol=5, axis=1)
        
    mdf["Trading Period"] = mdf.index.map(nzem.tp_tsagg)
    mdf["Season"] = mdf.index.map(nzem.season_tsagg)
    mdf["DOY"] = mdf.index.map(nzem.doy_tsagg)
    
    mdf = nzem.merge_dfseries(mdf, decile_hydro_level, left_on="DOY")
    mdf["Relative Level"] = mdf["Daily Stored"] - mdf["Decile Storage"]
    mdf["Season Name"] = mdf["Season"].map(season_map)
    mdf["Time Name"] = mdf["Trading Period"].map(period_map)
    
    mdf["NI Model Prediction"] = mdf.apply(get_probability, mod=nimod, axis=1)
    mdf["SI Model Prediction"] = mdf.apply(get_probability, mod=simod, axis=1)
    mdf["Sc NI Mod"] = mdf["NI Model Prediction"] * 0.82
    mdf["Sc SI Mod"] = mdf["SI Model Prediction"] * 0.9
    
    nitest = prob_assesment(mdf, "Sc NI Mod", "NI Constraint")
    sitest = prob_assesment(mdf, "Sc SI Mod", "SI Constraint")
    
    def monte_carlo(df, col="", N=1000, scale=1.0, ge=0.05):
        prediction = []
        pred = df[col]
        pred = pred * scale
        pred = pred.ge_mask(ge)
        for n in xrange(N):
            prediction.append(np.where(pred >= np.random.random(len(pred)), 1, 0).sum())
        
        return pd.Series(prediction)
            
    def sample_prob(series, mod=None):
        try:
            # Iteratively reduce the sample
            n = mod.eq_mask("Season", series["Season Name"])
            n = n.eq_mask("Time", series["Time Name"])
            n = n.ge_mask("Lake Level", series["Relative Level"])
            n = n.ix[0]["Weighted Probability"]
        except IndexError:
            n = np.nan
        return n
        
    def monte_assess(df, mod, col1, col2, N=1000, scale=1.0):
        n = np.arange(0, 1.0, 0.01)
        num = []
        for i in n:
            act = df.ge_mask(col1, i).eq_mask(col2, True)[col2].sum()
            pred = monte_carlo(df, mod, col=col1, N=N, scale=scale, ge=i)
            try:
                r = 1. -  (1.*(pred >= act).sum() / len(pred))
            except:
                r = 0
            num.append(r)
        return pd.Series(num, index=n)
        
    def monte_agg(df, col="", agg=None, N=1000, scale=1.0):
        n = np.arange(0, 1.0, 0.01)
        l = []
        for i in n:
            l.append(agg(monte_carlo(df, col=col, N=N, scale=scale, ge=i)))
        
        return pd.Series(l, index=n)
        
    #nitest_monte = monte_assess(mdf, nimod, "NI Model Prediction", "NI Constraint", N=1000, scale=0.82)
    #sitest_monte = monte_assess(mdf, simod, "SI Model Prediction", "SI Constraint", N=1000, scale=0.9)
    
    ### 2013 Model Test
    
    rp2013 = nzem.load_csvfile('2013_model_data/reserve_prices_2013.csv', trading_period_id=True)
    ep2013 = nzem.load_csvfile('2013_model_data/energy_prices_2013.csv', date_period=True)
    hd2013 = nzem.load_csvfile('2013_model_data/hydro_levels_2013.csv', niwa_date=True)["2013"]
    
    ms2013 = nzem.columnise_res_prices(rp2013)
    hay2013 = ep2013.eq_mask("Bus Id", "HAY2201")["Price Sum"]
    hay2013.name = "HAY2201 Price"
    ben2013 = ep2013.eq_mask("Bus Id", "BEN2201")["Price Sum"]
    ben2013.name = "BEN2201 Price"
    ms2013 = ms2013.merge_series(hay2013, left_index=True)
    ms2013 = ms2013.merge_series(ben2013, left_index=True)
    level2013 = hd2013["Daily Stored"].resample("30Min", fill_method='ffill')
    ms2013 = ms2013.merge_series(level2013, left_index=True)
    
    ms2013["DOY"] = ms2013.index.map(nzem.doy_tsagg)
    ms2013["Season"] = ms2013.index.map(nzem.season_tsagg)
    ms2013["Trading Period"] = ms2013.index.map(nzem.tp_tsagg)   
    
    ms2013["Season Name"] =  ms2013["Season"].map(season_map)
    ms2013["Time Name"] = ms2013["Trading Period"].map(period_map)
    
    ms2013 = ms2013.merge_series(decile_hydro_level, left_on="DOY")
    ms2013["Relative Level"] = ms2013["Daily Stored"] - ms2013["Decile Storage"]
    
    ms2013["NI Constraint"] = ms2013.apply(nzem.HVDC_Constraint,
        energy_price_send="BEN2201 Price", energy_price_receive="HAY2201 Price",
        res_price="NI Reserve Price", abs_tol=5, axis=1)
        
    ms2013["SI Constraint"] = ms2013.apply(nzem.HVDC_Constraint,
        energy_price_receive="BEN2201 Price", energy_price_send="HAY2201 Price",
        res_price="SI Reserve Price", abs_tol=5, axis=1)
        
    ms2013["NI Probability"] = ms2013.apply(get_probability, mod=nimod, axis=1)
    ms2013["SI Probability"] = ms2013.apply(get_probability, mod=simod, axis=1)
    
    ms2013["Sc NI Probability"] = ms2013["NI Probability"] * 0.82
    ms2013["Sc SI Probability"] = ms2013["SI Probability"] * 0.9
    
    def accuracy(ms, con_name, prob_name):
        n = np.arange(0, 1, 0.01)
        sc = []
        for i in n:
            sc.append([
           ms.ge_mask(prob_name, i)[prob_name].sum(),
           ms.ge_mask(prob_name, i).eq_mask(con_name, True)["DOY"].count()])
       
        scale = pd.DataFrame(sc, index=n, columns=["Model Prediction", "Actual"])
        scale["Percentage Difference"] = 100. * scale["Model Prediction"] / scale["Actual"] - 100
        scale["Difference"] = scale["Model Prediction"] - scale["Actual"]
        return scale
        
    niacc = accuracy(ms2013, "NI Constraint", "Sc NI Probability")
    siacc = accuracy(ms2013, "SI Constraint", "Sc SI Probability")
    
    fig = forecast_plot(niacc[:60])
    fig.savefig("ni_mod_pred.png", dpi=150)
    fig = forecast_plot(siacc[:25])
    fig.savefig("si_mod_pred.png", dpi=150)
    
    # Couple time series
    
    t1 = masterset[["NI Constraint", "NI Model Prediction"]]
    t1["Sc NI Probability"] = t1["NI Model Prediction"] * 0.82
    
    t2 = masterset[["SI Constraint", "SI Model Prediction"]]
    t2["Sc SI Probability"] = t2["SI Model Prediction"] * 0.9
    
    t3 = ms2013[["NI Constraint", "Sc NI Probability"]]
    t4 = ms2013[["SI Constraint", "Sc SI Probability"]]
    
    # NI Plot
    
    def month_year(x):
        return datetime.date(x.year, x.month, 1)
    
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    t1["NI Constraint"].groupby(month_year).sum().plot(ax=axes, style='k.-')
    t1["Sc NI Probability"].groupby(month_year).sum().plot(ax=axes, style='b.-')
    
    t3["NI Constraint"].groupby(month_year).sum().plot(ax=axes, style='k.--')
    t3["Sc NI Probability"].groupby(month_year).sum().plot(ax=axes, style='b.--')
    axes.set_xlim(733000.0, 734989.0)
    
    axes.set_ylabel("Number of Periods")
    
    fig.savefig('nimodelts.png', dpi=150)
    
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    t2["SI Constraint"].groupby(month_year).sum().plot(ax=axes, style='k.-')
    t2["Sc SI Probability"].groupby(month_year).sum().plot(ax=axes, style='b.-')
    
    t4["SI Constraint"].groupby(month_year).sum().plot(ax=axes, style='k.--')
    t4["Sc SI Probability"].groupby(month_year).sum().plot(ax=axes, style='b.--')
    axes.set_xlim(733000.0, 734989.0)
    
    axes.set_ylabel("Number of Periods")
    
    fig.savefig("simodelts.png", dpi=150)
    
    def rprice_prob(ms, price, const):
        n = np.arange(200, 1000, 10)
        sc = []
        for i in n:
            sc.append([ms.ge_mask(price, i).eq_mask(const, True)["DOY"].count(),
                       ms.ge_mask(price, i)["DOY"].count()])
        df = pd.DataFrame(sc, index=n, columns=["Constrained", "All"])
        df["Percentage"] = 1.0 * df["Constrained"] / df["All"]
        return df
        
    niprobprice = rprice_prob(masterset, "HAY2201 Price", "NI Constraint")
    siprobprice = rprice_prob(masterset, "BEN2201 Price", "SI Constraint") 
