import pandas as pd
import numpy as np
import sys
import os
import matplotlib.pyplot as plt
import datetime as dt
from dateutil.parser import parse

sys.path.append(os.path.join(os.path.expanduser("~"), 
                'python', 'pdtools'))
import pdtools


class ILOffer(object):
    """
    ILOffer
    ===========
    
    Wrapper around an IL Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, df):
        super(ILOffer, self).__init__()
        self.df = df
        self.stacked_frame = None
        self.single_frame = None


    def stack_frame(self):
        """
        Aggregates all of the frames together and adds them to a 
        stacked frame object.
        """

        self.stacked_frame = pd.concat(self._yield_stack_frame())


    def plot_offer_stack(self):

        if not self.single_frame:
            self.get_single_frame()

        fir = self.single_frame.eq_mask("Reserve Type", "6S")
        sir = self.single_frame.eq_mask("Reserve Type", "60S")

        fir.sort(columns=["PRICE", "MAX"], inplace=True)
        sir.sort(columns=["PRICE", "MAX"], inplace=True)

        fig, axes = plt.subplots(1,1,figsize=(16,9))

        axes.plot(fir["MAX"].cumsum(), fir["PRICE"], 'k.-', 
            label="FIR Offer Stack")
        axes.plot(sir["MAX"].cumsum(), sir["PRICE"], 'k.--', 
            label="SIR Offer Stack")

        axes.legend(loc='upper left')

        axes.set_xlabel("Offer [MW]")
        axes.set_ylabel("Price [$/MWh]")

        return fig, axes

    def get_single_frame(self, trading_date=None, 
                               trading_period=None,
                               reserve_type=None):

        if trading_date == None:
            trading_date = input(
                    "Please enter the trading date (day first): ")
        
        if trading_period == None:
            trading_period = input(
                    "Please enter the trading period (1-48): ")
        
        if reserve_type == None:
            reserve_type = input(
                    "Please enter the reserve type (6S, 60S): ")

        if type(trading_date) != dt.datetime:
            trading_date = parse(trading_date, dayfirst=True)

        if type(trading_period) != int:
            trading_period = int(trading_period)

        strdate = trading_date.strftime("%Y-%m-%d")

        self.single_frame = self.stacked_frame.eq_mask(
                "TRADING_DATE", strdate).eq_mask(
                "TRADING_PERIOD", trading_period).eq_mask(
                "Reserve Type", reserve_type)




    def _yield_stack_frame(self):
        """
        Generator function which will yield individual elements
        of the DataFrame instead of the horizontally stacked
        representation currently implemented.
        Is passed to a concatenation function in stack frames.
        """

        types = ("6S", "60S")
        pairs = ("MAX", "PRICE")

        general = [x for x in self.df.columns if "BAND" not in x]
        bands = [x for x in self.df.columns if x not in general]

        for ty in types:
            ty_bands = [x for x in bands if ty in x]
            prices = [x for x in ty_bands if "PRICE" in x]
            maxes = [x for x in ty_bands if "MAX" in x]

            for i, (price, maxe) in enumerate(zip(prices, maxes)):
                col_names = general + [price] + [maxe]
                single = self.df[col_names].copy()
                single.rename(columns={price: "PRICE", maxe: "MAX"}, 
                                inplace=True)
                # Set two new columns with the band and reserve type
                single["Reserve Type"] =  ty
                single["Band"] = i + 1
                yield single


    def _map_location(self, island=True, region=True):
        """
        Map the location based upon the node.
        Useful when looking at regional instances
        """

        pass







class PLSROffer(object):
    """
    PLSROffer
    ===========
    
    Wrapper around an PLSR Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, df):
        super(PLSROffer, self).__init__()
        self.df = df



class EnergyOffer(object):
    """
    EnergyOffer
    ===========
    
    Wrapper around an Energy Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, df):
        super(EnergyOffer, self).__init__()
        self.df = df

