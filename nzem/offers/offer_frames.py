"""A collection of classes to make working with energy and reserve offers
from the New Zealand Electricity Market a bit more painless.

Contains a master class, Offer, which publically exposed subclasses
inherit from.
This master class contains the code necessary for the exposed subclasses
to function.
"""

# Standard Library
import sys
import os
import datetime as dt
from dateutil.parser import parse
from collections import defaultdict
from datetime import datetime, timedelta
import collections
import functools
import itertools

# Non C Dependencies
import simplejson as json

# C Dependencies
import pandas as pd
import numpy as np


sys.path.append(os.path.join(os.path.expanduser("~"),
                'python', 'pdtools'))
try:
    import pdtools
except:
    print "Failed to import pdtools"

try:
    CONFIG = json.load(open(os.path.join(
        os.path.expanduser('~/python/nzem/nzem/_static/'), 'config.json')))
except:
    print "CONFIG File does not exist"

class Offer(object):
    """The Master Offer Class"""
    def __init__(self, offers, run_operations=True):
        """ Create the offer class by passing an offer DataFrame, optionally
        run a number of modifications

        Parameters
        ----------
        offers : type Pandas DataFrame
            A Pandas DataFrame containing offer data
        run_operations : type bool
            Run the operations on the contained offers

        Returns
        -------
        Offer : type Offer
            A container around a Pandas DataFrame containing additional
            functionality

        """
        super(Offer, self).__init__()
        self.offers = offers.copy()
        self.offer_stack = None
        if run_operations:
            self._retitle_columns()
            self._convert_dates()
            self._map_location()
            self._apply_datetime()
            self._sort_offers()
            self.offers.index = np.arange(len(self.offers))

    def stack_columns(self):
        """ Stack a horizontal dataframe into a vertical configuration to
        improve functionality

        Exists as an exposed wrapper around the _stacker() class which
        is a python generator yielding stacked DataFrames.

        Returns
        -------
        self.offer_stack : type Pandas DataFrame
            A DataFrame containing offer data with identifiers that has
            been stacked vertically


        """
        self.offer_stack = pd.concat(self._stacker(), ignore_index=True)


    def filter_dates(self, begin_date=None, end_date=None,
            horizontal=False, inplace=True, return_df=False):
        """ Filter either the Offer DataFrame or Stacked Offer frame
        between two dates.

        Parameters
        ----------

        begin_date: str, datetime, default None
            The first date to take, inclusive
        end_date: str, datetime, default None
            The last date to take, inclusive
        horizontal: bool, default False
            Whether to apply to the stacked offer frame or the horizontal offer frame
        inplace: bool, default True
            Overwrite the current offer with the new one
        return_df: bool, default False
            Return the filtered result to the user.

        Returns
        -------

        offer: DataFrame
            A subset of the initial offers for the date range specified.

        """

        if horizontal:
            offers = self.offers
        else:
            offers = self.offer_stack

        offers = offers.ge_mask("Trading Date", begin_date).le_mask("Trading Date", end_date)

        if inplace:
            if horizontal:
                self.offers = offers
            else:
                self.offer_stack = offers

        if return_df:
            return offers


    def filter_stack(self, date=None, period=None, product_type=None,
                     reserve_type=None, island=None, company=None,
                     region=None, station=None, non_zero=False,
                     return_df=False):
        """Filter a vertically stacked offer dataframe to obtain a
        subset of the data within it.

        Parameters
        ----------
        self.offer_stack : Pandas DataFrame
            Stacked data, if it does not exist it will be created
        date : str, bool default None
            The trading date to filter by
        period : str, bool default None
            The Trading Period to filter by
        product_type : str, bool default None
            Which product, IL, PLSR, TWDSR, Energy to filter by
        reserve_type : str, bool default None
            FIR, SIR or Energy, which reserve type to use
        island : str, bool default None
            Whether to filter by a specific Island e.g. (North Island)
        company : str, bool default None
            Filter a specific company (e.g. MRPL)
        region : str, bool, default None
            Filter a specific region (e.g. Auckland)
        station : str, bool, default None
            Which Station to filter, Generators optionally
        non_zero : bool, default False
            Return only non zero offers
        return_df : bool, default False
            Return the filtered DataFrame as well as saving to latest query

        Returns
        -------
        fstack : Pandas DataFrame
            The filtered DataFrame
        self.fstack : Pandas DataFrame
            The filtered DataFrame applied to a class method

        """


        if not isinstance(self.offer_stack, pd.DataFrame):
            self.stack_columns()

        fstack = self.offer_stack.copy()

        if date:
            fstack = fstack.eq_mask("Trading Date", date)

        if period:
            fstack = fstack.eq_mask("Trading Period", period)

        if product_type:
            fstack = fstack.eq_mask("Product Type", product_type)

        if reserve_type:
            fstack = fstack.eq_mask("Reserve Type", reserve_type)

        if island:
            fstack = fstack.eq_mask("Island", island)

        if company:
            fstack = fstack.eq_mask("Company", company)

        if region:
            fstack = fstack.eq_mask("Region", region)

        if station:
            fstack = fstack.eq_mask("Station", station)

        if non_zero:
            fstack = fstack.gt_mask("Max", 0)

        self.fstack = fstack

        if return_df:
            return fstack

    def clear_offer(self, requirement, fstack=None, return_df=True):
        """ Clear the offer stack against a requirement

        Parameters
        ----------
        self.fstack : pandas DataFrame
            The filter query, must be for a single period and date
        requirement : float
            The requirement for energy or reserve, must be a positive number
        fstack : pandas DataFrame, bool default None
            Optional argument to not use the current query
        return_df : bool, default True
            Return the DataFrame to the user, or keep as query

        Returns
        -------
        cleared_stack : DataFrame
            A DataFrame which has been cleared against the requirement
        uncleared_stack: DataFrame
            A DataFrame containing the offers which were not cleared

        """

        if not isinstance(fstack, pd.DataFrame):
            fstack = self.fstack.copy()

        if len(fstack) == 0:
            raise ValueError("fstack must be a DataFrame, \
                                current size is zero")

        if requirement <= 0:
            return (None, fstack)

        # Drop all non-zero offers
        fstack = fstack[fstack["Max"] > 0]

        # Sort by price
        fstack = fstack.sort(columns=["Price"])

        # Cumulative Offer
        fstack["Cumulative Offer"] = fstack["Max"].cumsum()

        # Reindex
        fstack.index = np.arange(len(fstack))

        # Get marginal unit index
        try:
            marginal_unit = fstack[fstack["Cumulative Offer"] >= requirement].index[0]
        except:
            marginal_unit = fstack.iloc[-1].name

        # Get the stacks
        cleared_stack = fstack.iloc[:marginal_unit+1].copy()
        uncleared_stack = fstack.iloc[marginal_unit:].copy()

        # Change the marginal unit to reflect the true params
        remain = uncleared_stack["Cumulative Offer"][marginal_unit] - requirement

        uncleared_stack["Max"][marginal_unit] = remain
        cleared_stack["Max"][marginal_unit] = cleared_stack["Max"][marginal_unit] - remain

        return cleared_stack, uncleared_stack


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
        return "FIR" if "6S" in band else "SIR" if "60S" in band else "Energy"

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
            user_map = pd.read_csv(CONFIG['map-location'])
            user_map = user_map[["Node", "Load Area", "Island Name",
                                 "Generation Type"]]
            user_map.rename(columns={"Node": "Grid Exit Point",
                "Load Area": "Region", "Island Name": "Island"}, inplace=True)

        self.offers = self.offers.merge(user_map, left_on=left_on, right_on=right_on)


    def _convert_dates(self, date_col="Trading Date"):

        self.offers[date_col] = pd.to_datetime(self.offers[date_col])


    def _apply_datetime(self, date_col="Trading Date",
            period_col="Trading Period", datetime_col="Trading Datetime"):

        period_map = {x: self._period_minutes(x) for x in set(self.offers[period_col])}

        self.offers[datetime_col] = self.offers[date_col] + self.offers[period_col].map(period_map)


    def _period_minutes(self, period):
        return timedelta(minutes=int(period)*30 -15)

    def _sort_offers(self, datetime_col="Trading Datetime"):
        self.offers.sort(columns=[datetime_col], inplace=True)





class ILOffer(Offer):
    """ Wrapper around an IL Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """

    def __init__(self, offers):
        super(ILOffer, self).__init__(offers)


    def merge_stacked_offers(self, plsr_offer):

        if not isinstance(self.offer_stack, pd.DataFrame):
            self.stack_columns()

        if not isinstance(plsr_offer.offer_stack, pd.DataFrame):
            plsr_offer.stack_columns()

        return ReserveOffer(pd.concat([self.offer_stack,
                    plsr_offer.offer_stack], ignore_index=True))



class PLSROffer(Offer):
    """ Wrapper around an PLSR Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, offers):
        super(PLSROffer, self).__init__(offers)


    def merge_stacked_offers(self, il_offer):

        if not isinstance(self.offer_stack, pd.DataFrame):
            self.stack_columns()

        if not isinstance(il_offer.offer_stack, pd.DataFrame):
            il_offer.stack_columns()

        return ReserveOffer(pd.concat([self.offer_stack,
                    il_offer.offer_stack], ignore_index=True))



class ReserveOffer(Offer):
    """ Container for mixed PLSR, IL and TWDSR Offers.
    Created by using the merge offers method of either the ILOffer
    or PLSROffer classes.
    """
    def __init__(self, offers):
        super(ReserveOffer, self).__init__(offers, run_operations=False)

        # Note, a raw Offer frame isn't passed, therefore manually add it
        # to the offer stack
        self.offer_stack = offers

    def NRM_Clear(self, fstack=None, max_req=0, ni_min=0, si_min=0):
        """ Clear the reserve offers as though the National Reserve Market
        was in effect. In effect will clear the market three times, with
        a reduced offer DataFrame each time.

        Note fstack must have been filtered already.

        Parameters
        ----------
        fstack: DataFrame, default None
            If desired don't use the "saved" filtered stack
        max_req: int, float, default 0
            The maximum requirement for reserves across the whole country.
        ni_min: int, float, default 0
            The North Island minimum requirement for reserve
        si_min: int, float, default 0
            The South Island minimum requirement for reserve

        Returns
        -------
        all_clear: DataFrame
            DataFrame which contains only the units which were cleared
            in the dispatch along with the respective North and South
            Island prices.

        Usage
        -----
        Note, this solver can be used to run a number of different
        possibilities depending upon the requirements passed.
        This improves the utility and flexibility of the method
        substantially, see the examples below for how to do this.
        The mixed market approach is intended for assessing a national
        reserve market in the presence of an HVDC link.

        Example One: Pure National Reserve Market
        >>> NRM_Clear(max_req=requirement, ni_min=0, si_min=0)

        Example Two: Stand alone Reserve Market
        >>> NRM_clear(max_req=0, ni_min=ni_risk, si_min=si_risk)

        Example Three: Mixed Market (exporting island reserve cannot
                                     securve importing island risk)
        >>> NRM_Clear(max_req=max_risk, ni_min=ni_hvdc, si_min=si_hvdc)

        """

        if not isinstance(fstack, pd.DataFrame):
            fstack = self.fstack.copy()

        national_requirement = max(max_req - ni_min - si_min, 0.)

        (nat_clear, nat_remain) = self.clear_offer(requirement=national_requirement, fstack=fstack)

        # I don't think this section is write, hence commenting out for now
        # # Calculate how much has been cleared from each island
        # if isinstance(nat_clear, pd.DataFrame):
        #     ni_cleared = nat_clear.eq_mask("Island", "North Island")["Max"].sum()
        #     si_cleared = nat_clear.eq_mask("Island", "South Island")["Max"].sum()
        # else:
        #     ni_cleared = 0
        #     si_cleared = 0

        # # Adjust the minimum requirements appropriately
        # ni_min = max(ni_min - ni_cleared, 0)
        # si_min = max(si_min - si_cleared, 0)

        # Calculate the new stacks
        ni_stack = nat_remain.eq_mask("Island", "North Island")
        si_stack = nat_remain.eq_mask("Island", "South Island")

        # Clear each island individually
        (ni_clear, ni_remain) = self.clear_offer(requirement=ni_min,
                fstack=ni_stack)
        (si_clear, si_remain) = self.clear_offer(requirement=si_min,
                fstack=si_stack)

        # Set the prices
        nat_price = 0
        if isinstance(nat_clear, pd.DataFrame):
            nat_price = max(nat_clear.iloc[-1]["Price"],0)

        if isinstance(ni_clear, pd.DataFrame):
            ni_price = max(ni_clear.iloc[-1]["Price"],nat_price)
        else:
            ni_price = nat_price

        if isinstance(si_clear, pd.DataFrame):
            si_price = max(si_clear.iloc[-1]["Price"],nat_price)
        else:
            si_price = nat_price

        all_clear = pd.concat((nat_clear, ni_clear, si_clear), ignore_index=True)
        all_clear["NI Price"] = ni_price
        all_clear["SI Price"] = si_price

        return all_clear

    def clear_all_NRM(self, clearing_requirements):
        """ Clear all of the NRM for a particular Reserve Offer
        DataFrame. Will iterate through all reserve types,
        trading dates and trading periods and match these with requirements
        for solving as a NRM.

        Parameters
        ----------
        self: ReserveOffer object
        clearing_requirements: DataFrame
            Multiindexed DataFrame containing the requirements for
            reserve both in total and for each island.

        Returns
        -------
        NRM_Cleared_Stack: DataFrame
            A DataFrame containing information for all units which were
            cleared in the operation of a NRM.


        """
        combinations = list(itertools.product(
                    self.offer_stack["Trading Date"].unique(),
                    self.offer_stack["Trading Period"].unique(),
                    self.offer_stack["Reserve Type"].unique()
                            ))

        return pd.concat(self._yield_NRM_result(clearing_requirements,
                             combinations), ignore_index=True)



    def _yield_NRM_result(self, clearing_requirements, combinations):
        """ Generator to calculate the solved solution to the NRM
        result to be computationally lazy.

        Parameters
        ----------
        self: class
            To perform the Filter assessment on
        clearing_requirements: DataFrame
            DataFrame with a multi index of (date, period, reserve_type)
            Is index to determine the requirements
        combinations: list
            List of all combinations to assess

        Returns
        -------
        nrm_solution: DataFrame
            The solution to a single iteration of the NRM clearer
            Is a generator object to feed into a concatenation

        """

        for (date, period, reserve_type) in combinations:

            # Get the requirements
            try:
                max_req, ni_min, si_min = clearing_requirements.ix[date].ix[period].ix[reserve_type].values
            except:
                yield None

            # Filter the data
            fstack = self.filter_stack(date=date, period=period,
                                        reserve_type=reserve_type)

            # Run the NRM Solver
            nrm_solution = self.NRM_Clear(fstack=fstack, max_req=max_req,
                        ni_min=ni_min, si_min=si_min)

            yield nrm_solution


class EnergyOffer(Offer):
    """ Wrapper around an Energy Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, offers):
        super(EnergyOffer, self).__init__(offers)


if __name__ == '__main__':
    pass
