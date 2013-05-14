import pandas as pd
import nzem
import numpy as np

def part_match(x, colset):
    for item in colset:
        if item in x:
            return True
    
    return False


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
    #nfr_data = nzem.load_csvfile("All_NFR_Data.csv", tpid="TRADING_PERIOD_ID",
    #                             trading_period_id=True)
                                 
    # Get hydro data
    hvdc_data = nfr_data.eq_mask("Risk Six Sec Plant", "HVDC").eq_mask("Risk Sixty Sec Plant", "HVDC")

    #col_set = ["Nfr", "Reserve", "Risk", "Mw"]            
    #cols = [x for x in hvdc_data.columns if part_match(x, col_set)]
    #    ghvdc_data = hvdc_data.groupby(["Island Name", "Date Time"])[cols].mean()
    #ghvdc_data = nfr_data.groupby(["Island Name", "Date Time"])[cols].max()

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

    ni_masterset = masterset.eq_mask("NI Constraint", True)
    si_masterset = masterset.eq_mask("SI Constraint", True)
    
    nihydro = nzem.load_csvfile("hydro_data/NIHydro.csv", niwa_date=True)
    sihydro = nzem.load_csvfile("hydro_data/SIHydro.csv", niwa_date=True)
    nilow10 = nzem.ts_aggregation(nihydro, "Ni Daily Stored", 
                ts_agg=nzem.doy_tsagg, agg=nzem.per10)
    silow10 = nzem.ts_aggregation(sihydro, "Si Daily Stored", 
                ts_agg=nzem.doy_tsagg, agg=nzem.per10)
    
    nilow10.name = "Ni Lower Decile"
    silow10.name = "Si Lower Decile"
    
    nihydro["DOY"] = nihydro.index.map(nzem.doy_tsagg)
    sihydro["DOY"] = sihydro.index.map(nzem.doy_tsagg)
    
    nihydro_all = nzem.merge_dfseries(nihydro, nilow10, left_on="DOY")
    sihydro_all = nzem.merge_dfseries(sihydro, silow10, left_on="DOY")
    
    nihydro_all["Ni Relative Level"] = nihydro_all["Ni Daily Stored"] - nihydro_all["Ni Lower Decile"]
    sihydro_all["Si Relative Level"] = sihydro_all["Si Daily Stored"] - sihydro_all["Si Lower Decile"]
    
    nih = nihydro_all[["Ni Daily Stored", "Ni Relative Level"]]
    sih = sihydro_all[["Si Daily Stored", "Si Relative Level"]]
    
    nih = nih.resample("30Min", fill_method='ffill')
    sih = sih.resample("30Min", fill_method='ffill')
    
    nmasterset = masterset.merge(nih, left_index=True, right_index=True)
    nmasterset = nmasterset.merge(sih, left_index=True, right_index=True)
    
    nin_masterset = nmasterset.eq_mask("NI Constraint", True)
    sin_masterset = nmasterset.eq_mask("SI Constraint", True)
    
    # North Island Hex plot
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    
    def hexplot(x, y, cmap=cm.binary, gridsize=20, **kargs):
        fig, ax = plt.subplots(1, 1, figsize=(16,9))
        hb = ax.hexbin(x, y, cmap=cmap, gridsize=gridsize, **kargs)
        ax.set_xlabel(x.name)
        ax.set_ylabel(y.name)
        fig.colorbar(hb)
        return fig, ax
    
    x1 = nin_masterset["Ni Daily Stored"]
    x2 = nin_masterset["Si Daily Stored"]
    
    hexplot(x1, x2)
    
    x3 = nin_masterset["Ni Relative Level"]
    x4 = nin_masterset["Si Relative Level"]
    
    hexplot(x3, x4)
    
    x5 = sin_masterset["Ni Daily Stored"]
    x6 = sin_masterset["Si Daily Stored"]
    
    hexplot(x5, x6)
    
    x7 = sin_masterset["Ni Relative Level"]
    x8 = sin_masterset["Si Relative Level"]
    
    hexplot(x7, x8)
    
    cut_rangeni = np.arange(-100, 1000, 30)
    cut_rangesi = np.arange(-1000, 5000, 30)
    
    x9 = nmasterset["Ni Relative Level"]
    x10 = nmasterset["Si Relative Level"]
    
    x11 = nmasterset["Ni Daily Stored"]
    x12 = nmasterset["Si Daily Stored"]
    
    def cut_count(x, cutrange):
        return 1. * pd.value_counts(pd.cut(x, cutrange))
    
    c1 = cut_count(x3, cut_rangeni) / cut_count(x9, cut_rangeni)
    c2 = cut_count(x4, cut_rangesi) / cut_count(x10, cut_rangesi)
    
    def rel_plot(x1, x2, x3, x4, bins=30, cmap=cm.binary):
        h, xedge, yedge = np.histogram2d(x1, x2, bins=bins)
        h1, xe, ye = np.histogram2d(x3, x4, bins=[xedge, yedge])
        
        hrel = h / h1
        hdf = pd.DataFrame(hrel, index=xedge[:-1], columns=yedge[:-1])
        hval = hdf.fillna(0).values
        
        fig, axes = plt.subplots(1, 1, figsize=(16,9))
        hb = axes.pcolorfast(xedge, yedge, hval.T, cmap=cmap)
        fig.colorbar(hb)
        axes.set_xlabel(x1.name)
        axes.set_ylabel(x2.name)
        return fig, axes
        
    # North Island
    import matplotlib
    fonts = {'family': 'serif', 'size': 16}
    matplotlib.rc('font', **fonts)
    
    fig, axes = rel_plot(x1, x4, x11, x10, cmap=cm.gist_yarg, bins=25)
    axes.set_xlabel("North Island Lake Level [GWh]")
    axes.set_ylabel("South Island Relative Lake Level [GWh]")
    axes.set_ylim(0, 2000)
    fig.savefig("nilakecompare.png", dpi=150)
    
    fig2, axes2 = rel_plot(x5, x8, x11, x10, cmap=cm.gist_yarg, bins=25)
    axes2.set_xlabel("North Island Lake Level [GWh]")
    axes2.set_ylabel("South Island Relative Lake Level [GWh]")
    fig2.savefig("silakecompare.png", dpi=150)
    
    # Try summer only
    
    summer = nmasterset
    xs1 = summer.eq_mask("NI Constraint", True)["Ni Daily Stored"]
    xs2 = summer.eq_mask("NI Constraint", True)["Si Relative Level"]
    
    xs3 = summer["Ni Daily Stored"]
    xs4 = summer["Si Relative Level"]
    
    fig, axes = rel_plot(xs1, xs2, xs3, xs4, cmap=cm.gist_yarg, bins=25)
    axes.set_xlabel("North Island Lake Level [GWh]")
    axes.set_ylabel("South Island Relative Lake Level [GWh]")
    axes.set_ylim(0, 2000)
#    fig.savefig("nilakecompare.png", dpi=150)
    
    fig, axes = plt.subplots(1, 2, sharey=True, figsize=(16,9))
    hb1 = axes[0].hexbin(xs1, xs2, gridsize=50, cmap=cm.binary)
    axes[0].hlines(y=1000, xmin=100, xmax=800, linestyles='dashed')
    axes[0].vlines(x=500, ymin=0, ymax=2000, linestyles='dashed')
    hb2 = axes[1].hexbin(xs3, xs4, gridsize=50, cmap=cm.binary)
    axes[1].hlines(y=1000, xmin=100, xmax=800, linestyles='dashed')
    axes[1].vlines(x=500, ymin=0, ymax=2000, linestyles='dashed')
    fig.colorbar(hb1, ax=axes[0])
    fig.colorbar(hb2, ax=axes[1])
    axes[0].set_ylim(0, 2000)
    axes[0].set_xlim(100, 800)
    axes[1].set_xlim(100, 800)
    axes[0].set_ylabel("South Island Relative Lake Level [GWh]")
    #fig.text(0.05, 0.5, "South Island Relative Lake Level [GWh]", ha='center', va='center', rotation='vertical')
    fig.text(0.5, 0.05, "North Island Lake Level [GWh]", ha='center', va='center')
    axes[0].set_title("Constrained Period Frequency")
    axes[1].set_title("All Period Frequency")
    fig.tight_layout(pad=1.02)
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
#    fig.savefig("niallcompare.png", dpi=150)
    
    
    summer = nmasterset
    xs1 = summer.eq_mask("SI Constraint", True)["Ni Daily Stored"]
    xs2 = summer.eq_mask("SI Constraint", True)["Si Relative Level"]

    xs3 = summer["Ni Daily Stored"]
    xs4 = summer["Si Relative Level"]

    fig, axes = rel_plot(xs1, xs2, xs3, xs4, cmap=cm.gist_yarg, bins=25)
    axes.set_xlabel("North Island Lake Level [GWh]")
    axes.set_ylabel("South Island Relative Lake Level [GWh]")
    axes.set_ylim(0, 2000)
    #    fig.savefig("nilakecompare.png", dpi=150)

    ymin=-750
    ymax=2000
    xmin=100
    xmax=800
    fig, axes = plt.subplots(1, 2, sharey=True, figsize=(16,9))
    hb1 = axes[0].hexbin(xs1, xs2, gridsize=50, cmap=cm.binary)
    axes[0].hlines(y=1000, xmin=xmin, xmax=xmax, linestyles='dashed')
    axes[0].vlines(x=500, ymin=ymin, ymax=ymax, linestyles='dashed')
    hb2 = axes[1].hexbin(xs3, xs4, gridsize=50, cmap=cm.binary)
    axes[1].hlines(y=1000, xmin=xmin, xmax=xmax, linestyles='dashed')
    axes[1].vlines(x=500, ymin=ymin, ymax=ymax, linestyles='dashed')
    fig.colorbar(hb1, ax=axes[0])
    fig.colorbar(hb2, ax=axes[1])
    axes[0].set_ylim(ymin, ymax)
    axes[0].set_xlim(xmin, xmax)
    axes[1].set_xlim(xmin, xmax)
    axes[0].set_ylabel("South Island Relative Lake Level [GWh]")
    #fig.text(0.05, 0.5, "South Island Relative Lake Level [GWh]", ha='center', va='center', rotation='vertical')
    fig.text(0.5, 0.05, "North Island Lake Level [GWh]", ha='center', va='center')
    axes[0].set_title("Constrained Period Frequency")
    axes[1].set_title("All Period Frequency")
    fig.tight_layout(pad=1.02)
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    #    fig.savefig("niallcompare.png", dpi=150)
