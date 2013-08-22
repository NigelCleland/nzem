import pandas as pd
import numpy as np
import sys
import os
import matplotlib.pyplot as plt
import datetime as dt
from dateutil.parser import parse
from collections import defaultdict
from pandas.tseries.offsets import Minute
import datetime as dt
from datetime import datetime, timedelta


sys.path.append(os.path.join(os.path.expanduser("~"),
                'python', 'pdtools'))
import pdtools

class Offer(object):
    """docstring for Offer"""
    def __init__(self, offers):
        super(Offer, self).__init__()
        self.offers = offers.copy()
        self._retitle_columns()
        self._convert_dates()
        self._map_location()
        self._apply_datetime()

    def stack_columns(self):
        self.offer_stack = pd.concat(self._stacker(), ignore_index=True)


    def _stacker(self):
        """ General Stacker designed to handle all forms of
        offer dataframe, energy, plsr, and IL
        """


        general_columns = [x for x in self.offers.columns if "Band" not in x]
        band_columns = [x for x in self.offers.columns if x not in general_columns]
        filterdict = self._assign_band(band_columns)

        for key in filterdict:

            all_cols = general_columns + filterdict[key].values()

            single = self.offers[all_cols].copy()
            # Assign identifiers
            single["Product Type"] = key[0]
            single["Reserve Type"] = key[1]
            single["Band Number"] = key[2]

            single.rename(columns={v: k for k, v in filterdict[key].items()},
                inplace=True)

            yield single


    def _assign_band(self, band_columns):
        """ Figure out what type of columns they are from the bands
        Should return a list of lists of the form

        Product Type, Reserve Type, Band, Params*
        """

        filtered = defaultdict(dict)
        for band in band_columns:
            split = band.split()

            band_number = int(split[0][4:])
            param = split[-1]
            reserve_type = self._reserve_type(band)
            product_type = self._product_type(band)

            filtered[(product_type, reserve_type, band_number)][param] = band

        return filtered

    def _reserve_type(self, band):
        return "FIR" if "6S" in band else "SIR" if "60S" in band else "N"

    def _product_type(self, band):
        return "PLSR" if (
                "Plsr" in band) else "TWDSR" if (
                "Twdsr" in band) else"IL" if (
                "6S" in band or "60S" in band) else "Energy"


    def _retitle_columns(self):
        self.offers.rename(columns={x: x.replace('_', ' ').title()
                for x in self.offers.columns}, inplace=True)


    def _map_location(self, user_map=None,
            left_on="Grid Exit Point", right_on="Grid Exit Point"):
        """
        Map the location based upon the node.
        Useful when looking at regional instances
        """

        if not user_map:
            user_map = pd.read_csv('/home/nigel/data/maps/Nodal_Information.csv')
            user_map = user_map[["Node", "Load Area", "Island Name"]]
            user_map.rename(columns={"Node": "Grid Exit Point",
                "Load Area": "Region", "Island Name": "Island"}, inplace=True)

        self.offers = self.offers.merge(user_map, left_on=left_on, right_on=right_on)


    def _convert_dates(self, date_col="Trading Date"):

        self.offers[date_col] = pd.to_datetime(self.offers[date_col])


    def _apply_datetime(self, date_col="Trading Date",
            period_col="Trading Period", datetime_col="Trading Datetime"):

        self.offers[datetime_col] = self.offers[date_col] + self.offers[period_col].apply(self._period_minutes)


    def _period_minutes(self, period):
        return timedelta(minutes=int(period)*30 -15)







class ILOffer(Offer):
    """
    ILOffer
    ===========

    Wrapper around an IL Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, offers):
        super(ILOffer, self).__init__(offers)



class PLSROffer(Offer):
    """
    PLSROffer
    ===========

    Wrapper around an PLSR Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, offers):
        super(PLSROffer, self).__init__()
        self.offers = offers



class EnergyOffer(Offer):
    """
    EnergyOffer
    ===========

    Wrapper around an Energy Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, offers):
        super(EnergyOffer, self).__init__()
        self.offers = offers

