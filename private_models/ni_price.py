import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import nzem

if __name__ == '__main__':

    res_prices = nzem.load_csvfile("reserve_prices.csv", trading_period_id=True)
    allr_prices = nzem.columnise_res_prices(res_prices)
    
    nia = nzem.reduced_aggregation(allr_prices["NI Reserve Price"], agg=np.sum, percent=True, npoints=892)
    nia2 = nia / 2.
    
    nia3 = 100. * (1. - nia2 / nia2.max())
    
    fonts = {'family': 'serif', 'size': 18}
    matplotlib.rc('font', **fonts)
    
    fig, axes = plt.subplots(1, 1, figsize=(16,9))
    
    nia2.plot(ax=axes, style="b:", linewidth=3)
    axes.grid(axis='y')
    axes.set_ylabel("Value of 1 MW of Reserve for [$/MW]", fontstyle='italic')
    axes.set_xlabel("Percentage of top periods missed", fontstyle='italic')
    
    for label in axes.get_yticklabels():
        label.set_rotation(30)
        
    def yformatter(x, pos):
        return "$\${{{:,.0f}}}$".format(int(x))
        
    def xformatter(x, pos):
        return "${{{0}}}\%$".format(x)
        
    axes.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(yformatter))
    axes.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(xformatter))
    
    axes2 = axes.twinx()
    nia3.plot(ax=axes2, style='r--', linewidth=3)
    axes2.grid()
    axes2.set_ylabel("Percentage of revenue missed", fontstyle='italic')
    
    def secyformatter(x, pos):
        return "${{{:.0f}}}\%$".format(x)
        
    axes2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(secyformatter))
    
    labels2 = axes2.get_yticklabels()
    labels2[0].set_fontsize(0)
    
    axes.set_xlim(0, 1)
    
    fig.savefig("reserveprice.png", dpi=150)
