import pandas as pd
import nzem
import numpy as np
import style
import matplotlib.pyplot as plt
import matplotlib

font = {'family': 'serif', 'size': 18}
matplotlib.rc('font', **font)

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
    
    ni_masterset = masterset.eq_mask("NI Constraint", True)
    si_masterset = masterset.eq_mask("SI Constraint", True)
    
    # TIME AGGREGATIONS
    ni_month_eq = nzem.conditional_probability(ni_masterset["Month"],
            masterset["Month"], step=1, how='eq')
    si_month_eq = nzem.conditional_probability(si_masterset["Month"],
            masterset["Month"], step=1, how='eq')
            
    ni_tp_eq = nzem.conditional_probability(ni_masterset["Trading Period"],
            masterset["Trading Period"], step=1, how='eq')
    si_tp_eq = nzem.conditional_probability(si_masterset["Trading Period"],
            masterset["Trading Period"], step=1, how='eq')
            
    ni_doy_eq = nzem.conditional_probability(ni_masterset["DOY"], masterset["DOY"],
        step=1, how='eq')
    
    si_doy_eq = nzem.conditional_probability(si_masterset["DOY"], masterset["DOY"],
        step=1, how='eq')
        
        
    ni_woy_eq = nzem.conditional_probability(ni_masterset["WOY"], masterset["WOY"],
        step=1, how='eq')
    
    si_woy_eq = nzem.conditional_probability(si_masterset["WOY"], masterset["WOY"],
        step=1, how='eq')
    
    
    
    # Lake Level
    ni_lake_ge = nzem.conditional_probability(ni_masterset["Relative Level"], 
                 masterset["Relative Level"], step=20)
    ni_lake_le = nzem.conditional_probability(ni_masterset["Relative Level"], 
                 masterset["Relative Level"], how='le', step=20)
    
    si_lake_ge = nzem.conditional_probability(si_masterset["Relative Level"], 
                 masterset["Relative Level"], step=20)
    si_lake_le = nzem.conditional_probability(si_masterset["Relative Level"], 
                 masterset["Relative Level"], how='le', step=20)
    
    
    # Demand
    
    ni_demand_ge = nzem.conditional_probability(ni_masterset["National Demand"], 
                 masterset["National Demand"], step=20)
    ni_demand_le = nzem.conditional_probability(ni_masterset["National Demand"], 
                 masterset["National Demand"], how='le', step=20)
                 
    si_demand_ge = nzem.conditional_probability(si_masterset["National Demand"], 
                 masterset["National Demand"], step=20)
    si_demand_le = nzem.conditional_probability(si_masterset["National Demand"], 
                 masterset["National Demand"], how='le', step=20)
    
    # Separate by island
    ni_hvdc_data = ghvdc_data.in_eqmask("Risk Six Sec Plant", ("HVDC", "GEN", "GENCE")).ix["NI"]
    si_hvdc_data = ghvdc_data.in_eqmask("Risk Six Sec Plant", ("HVDC", "GEN", "GENCE")).ix["SI"]
    
    ni_col = {x: " ".join(["NI", x]) for x in ni_hvdc_data.columns}
    si_col = {x: " ".join(["SI", x]) for x in si_hvdc_data.columns}
    
    ni_hvdc_data = ni_hvdc_data.rename(columns=ni_col)
    si_hvdc_data = si_hvdc_data.rename(columns=si_col)
    
    # Do outer merges
    
    masterset_outer = masterset.merge(ni_hvdc_data, left_index=True,
                                      right_index=True, how='outer')
    
    masterset_outer = masterset_outer.merge(si_hvdc_data, left_index=True,
                                      right_index=True, how='outer')
                        
    # Lazy typing              
    mo = masterset_outer
    nmo = masterset_outer.eq_mask("NI Constraint", True)
    smo = masterset_outer.eq_mask("SI Constraint", True)
    
    
    # DC Transfer
    
    ni_flow_ge = nzem.conditional_probability(nmo["NI Net Dc Transfer Mw"] * -1,
        mo["NI Net Dc Transfer Mw"] * -1, step=5, how='ge')
    
    si_flow_ge = nzem.conditional_probability(smo["SI Net Dc Transfer Mw"] * -1,
        mo["SI Net Dc Transfer Mw"] * -1, step=5, how='ge')
        
    # Risk
    ni_risks_ge = nzem.conditional_probability(nmo["NI Reserve Actual Sixty Sec Mw"],
        mo["NI Reserve Actual Sixty Sec Mw"], step=5, how='ge')
    
    si_risks_ge = nzem.conditional_probability(smo["SI Reserve Actual Sixty Sec Mw"],
        mo["SI Reserve Actual Sixty Sec Mw"], step=5, how='ge')
    
    ni_riskf_ge = nzem.conditional_probability(nmo["NI Reserve Actual Six Sec Mw"],
        mo["NI Reserve Actual Six Sec Mw"], step=5, how='ge')
    
    si_riskf_ge = nzem.conditional_probability(smo["SI Reserve Actual Six Sec Mw"],
        mo["SI Reserve Actual Six Sec Mw"], step=5, how='ge')
    
    # Reserve Availability
    
    ni_avail_ge = nzem.conditional_probability(nmo["NI Reserve Available Six Sec"],
        mo["NI Reserve Available Six Sec"], how='ge', step=5)
    
    ni_avail_le = nzem.conditional_probability(nmo["NI Reserve Available Sixty Sec"],
        mo["NI Reserve Available Sixty Sec"], how='le', step=5)
    
    si_avail_ge = nzem.conditional_probability(smo["SI Reserve Available Six Sec"],
        mo["SI Reserve Available Six Sec"], how='ge', step=5)
    
    si_avail_le = nzem.conditional_probability(smo["SI Reserve Available Sixty Sec"],
        mo["SI Reserve Available Sixty Sec"], how='le', step=5)

    # FIR/SIR availability Split
    
    
    # New Plots
    
    # Time Series
    fig, axes = plt.subplots(2, 1, figsize=(16,9), sharex=True)
    (mo["NI Constraint"].resample("W", how='sum') * 100.0 / mo["NI Constraint"].resample("W", how='count')).plot(ax = axes[0], style='k-') 
    (mo["SI Constraint"].resample("W", how='sum') * 100.0 / mo["SI Constraint"].resample("W", how='count')).plot(ax = axes[1], style='k--')
    for ax in axes:
        ax.grid()
        ax.grid(axis='y')
        ax.set_xlabel('')
        ax.set_ylabel('')
        
    for label in axes[1].get_xticklabels():
        label.set_rotation(30)
        
    fig.subplots_adjust(hspace=0.075)
    fig.text(0.075, 0.5, 'Weekly Frequency of Binding Reserve Constraints [%]',
            ha='center', va='center', rotation='vertical')
    for ax in axes:
        rstyle(ax)
    fig.savefig("constraint_time_series.png", dpi=150)
    
    # Monthly Plots
    smap = {1: "January", 2: "February", 3:"March", 4:"April", 5:"May", 6:"June",
            7: "July", 8: "August", 9: "September", 10: "October", 11: "November",
            12: "December"}

    ni_month_eq.name = "NI Monthly Frequency"
    si_month_eq.name = "SI Monthly Frequency"
    dfmonth = nzem.merge_series(ni_month_eq, si_month_eq)
    dfmonth["NI Monthly Frequency"] = dfmonth["NI Monthly Frequency"] - ni_month_eq.mean()
    dfmonth["SI Monthly Frequency"] = dfmonth["SI Monthly Frequency"] - si_month_eq.mean()
    dfmonth["Month"] = dfmonth.index
    dfmonth["Month"] = dfmonth["Month"].map(smap)
    dfmonth.index = dfmonth["Month"]
    dfmonth = dfmonth.dropna()
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    (dfmonth["NI Monthly Frequency"]*100.).plot(ax=axes, style='k-')
    (dfmonth["SI Monthly Frequency"]*100.).plot(ax=axes, style='k--')
    axes.grid()
    axes.grid(axis='y')
    axes.set_ylabel("Monthly frequency deviation of reserve constraints")
    for label in axes.get_xticklabels():
        label.set_rotation(30)
    rstyle(axes)
    fig.savefig("monthly_constraint_activity.png", dpi=150)
    
    # Trading Periods Plots
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    (100. * ni_tp_eq).plot(style='k-', ax=axes)
    (100. * si_tp_eq).plot(style='k--', ax=axes)
    axes.grid()
    axes.grid(axis='y')
    axes.set_xlabel("Trading Period")
    axes.set_ylabel("Frequency of binding reserve constraints [%]")
    rstyle(axes)
    fig.savefig("trading_period_activity.png", dpi=150)


    # Hydro Plots    
    fig, axes = plt.subplots(2, 1, figsize=(16,9), sharex=True)
    (100. * ni_lake_ge).plot(ax=axes[0], style='k-')
    (100. * si_lake_le).plot(ax=axes[1], style='k--')
    for ax in axes:
        ax.grid()
        ax.grid(axis='y')
        ax.set_xlabel('')
        ax.set_ylabel('')
        
    fig.text(0.5, 0.05, "New Zealand Relative Lake Level [GWh]", ha='center', va='center')
    fig.text(0.09, 0.5, "Conditional Probability of Reserve Constraint [%]",
        ha='center', va='center', rotation='vertical')
    plt.xlim(-1000, 2000)
    fig.subplots_adjust(hspace=0.075)
    for ax in axes:
        rstyle(ax)
    fig.savefig("hydro_situation.png", dpi=150)
    
    # Demand Plots
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    (100. * ni_demand_ge).plot(ax=axes, style='k-')
    (100. * si_demand_le).plot(ax=axes, style='k--')
    axes.grid()
    axes.grid(axis='y')
    axes.set_xlabel("National Demand [MW]")
    axes.set_ylabel("Conditional Probability of Reserve Constraint [%]")
    rstyle(axes)
    fig.savefig("demand_situation.png", dpi=150)
    
    # Risk plots
    fig, axes = plt.subplots(2, 1, figsize=(16, 9))
    (100. * ni_risks_ge).plot(ax=axes[0], style='k-')
    (100. * si_risks_ge).plot(ax=axes[1], style='k--')
    for ax in axes:
        ax.grid()
        ax.grid(axis='y')

    fig.text(0.5, 0.05, "Island Risk [MW]", ha='center', va='center')
    fig.text(0.075, 0.5, "Conditional Probability of Constraint", 
        ha='center', va='center', rotation='vertical')    
    for ax in axes:
        rstyle(ax)
    fig.savefig("risk_analysis.png", dpi=150)
    
    # Reserve availability plots
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    (100. * ni_avail_le).plot(ax=axes, style='k-')
    (100. * si_avail_le).plot(ax=axes, style='k--')
    axes.set_xlabel("Reserve Availability [MW]")
    axes.set_ylabel("Conditional Probability of Reserve Constraint [%]")
    axes.grid()
    axes.grid(axis='y')
    rstyle(axes)
    fig.savefig("reserve_avail.png", dpi=150)
    
