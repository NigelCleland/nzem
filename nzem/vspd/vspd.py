"""
Module to make working with vSPD data a lot simpler and easier
"""

# Standard Library Imports
import sys
import os
import datetime
import glob
import simplejson as json

# C Library Imports

import pandas as pd
import numpy as np

# Import nzem

# Get the config file
try:
    CONFIG = json.load(open(os.path.join(
    os.path.expanduser('~/python/nzem/nzem/_static'), 'config.json')))
except:
    print "CONFIG File does not exist"

class vSPUD_Factory(object):
    """docstring for ClassName"""
    def __init__(self, master_folder, patterns=None):
        """Initialise a vSPUD factory by passing a maser folder
        as well as a directory which contains vSPD sub directories.
        Can optionally pass a pattern to match on the sub directories

        Usage
        -----
        >>>> # Assume that the folder has sub directories with format
        >>>> # FP_yyyymmdd_identifier
        >>>> # To get all of 2009, January you could pass
        >>>> Factory = vSPUD_Factory(folder, pattern="200901")
        >>>> # To get all of a particular type
        >>>> Factory = vSPUD_Factory(folder, pattern="identifier_one")

        Parameters
        ----------
        master_folder: string
            An absolute path to a master folder for the vSPD results directory
        pattern: string, default None, optional
            An optional string to match the sub folders on


        Returns
        -------
        vSPUD_Factory: class
            A factory for creating vSPUD objects from the directories
        """

        super(vSPUD_Factory, self).__init__()

        self.master_folder = master_folder
        self.patterns = patterns
        # Recursively walk the directories, this handles a nested structure
        # if necessary to the factory operation
        self.sub_folders = [dire for (dire, subdir,
                files) in os.walk(master_folder) if files and '.csv' in files[0]]
        if patterns:
            self.match_pattern(patterns=patterns)


    def match_pattern(self, patterns):
        """
        Apply a new pattern to the directory structure

        Parameters
        ----------
        patterns: iterable
            Pass an iterable of patterns (an iterable of one is okay)
            to match the sub folders against

        Returns
        -------
        self.sub_folders: iterable
            Sub folders containing the vSPD files to load within the
            factory

        """

        folders = glob.glob(os.path.join(self.master_folder, '*'))
        folders = [x for x in folders if os.path.isdir(x)]
        subfolders = [x for x in folders if self._matcher(x, patterns)]
        self.sub_folders = subfolders


    def _matcher(self, a, p):
        for b in p:
            if b not in a:
                return False
        return True


    def load_results(self, island_results=None, summary_results=None,
                system_results=None, bus_results=None, reserve_results=None,
                trader_results=None, offer_results=None, branch_results=None):
        """ Load vSPUD objects with information from the folders as
        specified by the key word arguments above.

        Parameters
        ----------
        island_results: bool, default None, optional
        summary_results: bool, default None, optional
        system_results: bool, default None, optional
        bus_results: bool, default None, optional
        reserve_results: bool, default None, optional
        trader_results: bool, default None, optional
        offer_results: bool, default None, optional
        branch_results: bool, default None, optional

        Returns
        -------
        vSPUD: class
            A vSPUD object with information as defined by the Keyword arugments
            If a folder is passed the DataFrames can be fine tuned by passing
            a keyword arguement as according to the _load_data method.

        """

        if reserve_results:
            reserve_results = pd.concat((x.reserve_results for
                x in self._yield_results(reserve=True)),ignore_index=True)

        if island_results:
            island_results = pd.concat((x.island_results for
                x in self._yield_results(island=True)),ignore_index=True)

        if summary_results:
            summary_results = pd.concat((x.summary_results for
                x in self._yield_results(summary=True)),ignore_index=True)

        if system_results:
            system_results = pd.concat((x.system_results for
                x in self._yield_results(system=True)),ignore_index=True)

        if bus_results:
            bus_results = pd.concat((x.bus_results for
                x in self._yield_results(bus=True)),ignore_index=True)

        if trader_results:
            trader_results = pd.concat((x.trader_results for
                x in self._yield_results(trader=True)),ignore_index=True)

        if offer_results:
            offer_results = pd.concat((x.offer_results for
                x in self._yield_results(offer=True)),ignore_index=True)

        if branch_results:
            branch_results = pd.concat((x.branch_results for
                x in self._yield_results(branch=True)),ignore_index=True)

        return vSPUD(reserve_results=reserve_results,
                island_results=island_results, summary_results=summary_results,
                system_results=system_results, bus_results=bus_results,
                trader_results=trader_results, offer_results=offer_results,
                branch_results=branch_results)


    def _yield_results(self, **kargs):

        for folder in self.sub_folders:
            yield vSPUD(folder, **kargs)


class vSPUD(object):
    """docstring for vSPUD"""
    def __init__(self, folder=None, island_results=None, summary_results=None,
                system_results=None, bus_results=None, reserve_results=None,
                trader_results=None, offer_results=None, branch_results=None,
                 **kargs):
        """ Initialise a blank vSPUD object. It is intended to pass either:
        a) a folder containing vSPD results
        b) At least one of the *_results etc as a DataFrame

        if a folder is passed it will over write any other DataFrames passed.

        Usage:
        ------
        >>>> # Using from a folder
        >>>> A = vSPUD(folder=str, reserve=True, island=True)
        >>>> # DataFrames loads for reserve and island
        >>>> type(A.reserve_results) == DataFrame
        >>>> type(A.island_results) == DataFrame
        >>>> # All others are false
        >>>> type(A.bus_results) == None
        >>>> # Passing DataFrames
        >>>> B = vSPUD(island_results=df)
        >>>> type(B.island_results) == DataFrame
        >>>> type(B.reserve_results) == None

        Parameters
        ----------
        folder: str, default None, optional
            A string which contains the absolute path to a folder of vSPD
            results. Used in conjunction with **kargs.
        **kargs: dict, optional
            Optional key word arguments to pass to the _load_data function
            when initialising the vSPUD object from a folder

        island_results: DataFrame, default None, optional
        summary_results: DataFrame, default None, optional
        system_results: DataFrame, default None, optional
        bus_results: DataFrame, default None, optional
        reserve_results: DataFrame, default None, optional
        trader_results: DataFrame, default None, optional
        offer_results: DataFrame, default None, optional
        branch_results: DataFrame, default None, optional

        Returns
        -------
        vSPUD object: class
            A vSPUD object with information as defined by the Keyword arugments
            If a folder is passed the DataFrames can be fine tuned by passing
            a keyword arguement as according to the _load_data method.

        """

        super(vSPUD, self).__init__()

        self.island_results = island_results
        self.summary_results = summary_results
        self.system_results = system_results
        self.bus_results = bus_results
        self.reserve_results = reserve_results
        self.trader_results = trader_results
        self.offer_results = offer_results
        self.branch_results = branch_results

        if folder:
            self.folder = folder
            self._load_data(**kargs)

    def map_dispatch(self):
        """ Map the offer dispatch DataFrame to nodal metadata

        Parameters
        ----------
        self.offer_results: DataFrame
            The Offer DataFrame

        Returns
        -------
        mapoffers: DataFrame
            A DataFrame with the units mapped to location and generation type

        """

        # Map the Locations
        offers = self.offer_results.copy()
        mapoffers = self._map_nodes(offers, left_on="Offer")

        return mapoffers

    def dispatch_report(self):
        """ Construct a dispatch report based upon the Offer DataFrame,
        Can perform a variety of aggregations and grouping.

        Parameters
        ----------

        Returns
        -------
        report: DataFrame
            A DataFrame containing the specified report

        """

        pass


    def price_report(self):
        """ Create a brief aggregation of the Reference Energy
        and Reserve prices as well as the reference price differentials

        Parameters
        ----------
        self.island_results: DataFrame
            DataFrame of the Island Results

        Returns
        -------
        price_report: DataFrame
            DataFrame containing basic price information with precompiled
            aggregations

        Usage
        -----
        >>>>preport = spud.price_report()
        >>>>preport.head()

        """

        ni_prices = self.island_results[self.island_results["Island"] == "NI"]
        si_prices = self.island_results[self.island_results["Island"] == "SI"]

        columns = ["ReferencePrice ($/MWh)", "FIR Price ($/MWh)", "SIR Price ($/MWh)"]

        nip = ni_prices[["DateTime"] + columns].copy()
        sip = si_prices[["DateTime"] + columns].copy()

        nip.rename(columns={x: " ".join(["NI", x]) for x in columns}, inplace=True)
        sip.rename(columns={x: " ".join(["SI", x]) for x in columns}, inplace=True)

        price_report = nip.merge(sip, left_on="DateTime", right_on="DateTime")

        price_report["Island Price Difference ($/MWh)"] = price_report["NI ReferencePrice ($/MWh)"] - price_report["SI ReferencePrice ($/MWh)"]

        price_report["NI Reserve Price ($/MWh)"] = price_report["NI FIR Price ($/MWh)"] + price_report["NI SIR Price ($/MWh)"]
        price_report["SI Reserve Price ($/MWh)"] = price_report["SI FIR Price ($/MWh)"] + price_report["SI SIR Price ($/MWh)"]

        return price_report

    def reserve_procurement(self, overwrite_results=False, apply_time=False,
                            aggregation=None, agg_func=np.sum, **kargs):
        """ Calculate the reserve procurement costs and apply an optional
        aggregation to the calculated result. This aggregation can be general,
        and is applied to a groupby function as specified by a list containing
        the aggregation columns.

        Parameters
        ----------
        self.reserve_results: DataFrame
            The reserve results by trading periods
        overwrite_results: bool, default False, optional
            Whether to overwrite the original reserve_results DataFrame with
            one containing the procurement information
        apply_time: bool, default False, optional
            Apply the time aggregations if needed with details specified in
            **kargs
        **kargs: booleans,
            What optional time aggregation values to apply, defaults to all
            aggregations.
        aggregation: list, default None, optional
            A list of columns to aggregate the procurement by
        agg_func: function, default np.sum
            Aggregation function to apply to the result, must take many an
            array and return a single value

        Returns
        -------
        res_results: DataFrame
            A DataFrame containing the reserve procurement information
            with any aggregations applied

        Usage
        -----
        >>>> SPUD_Example.reserve_procurement(apply_time=True,
                        aggregation=["Island", "Month_Year"],
                        agg_func=None, month_year=True)
        >>>> # Returns procurement costs aggregated by month of year and island
        >>>> # For both FIR and SIR
        """

        if not self.reserve_results:
            raise ValueError("You must have created a vSPUD instance \
                              with a reserve results flag set")

        # Grab a copy of the DF
        res_results = self.reserve_results.copy()


        fir_cols = [x for x in res_results.columns if "FIR" in x and not "Violation" in x]
        sir_cols = [x for x in res_results.columns if "SIR" in x and not "Violation" in x]

        res_results["FIR Procurement ($)"] = res_results[fir_cols].product(axis=1)
        res_results["SIR Procurement ($)"] = res_results[sir_cols].product(axis=1)

        if apply_time:
            res_results = self._apply_time_filters(res_results, **kargs)

        if overwrite_results:
            self.reserve_results = res_results

        if aggregation:
            res_results = res_results.groupby(aggregation)[
                "FIR Procurement ($)",
                "SIR Procurement ($)"].aggregate(agg_func)

        return res_results


    def calculate_differentials(self, other, left_name="Control",
                           right_name="Override", diff_name="Difference",
                           diff_only=False, calc_type="reserve_results",
                           method="Subtract"):
        """ Determine the reserve comparison report for two vSPD iterations

        Parameters
        ----------
        self: class vSPUD
            A vSPUD class with reserve results present
        other: class vSPUD
            Another vSPUD class with reserve results present
        left_name: string, default "Control"
            Identifying name to apply to the original vSPUD object
        right_name: string, default "Override"
            Identifying name to apply to the second vSPUD object
        diff_name: string, default "Difference"
            Identifying name to apply to the difference
        diff_only: bool, default False
            If True, return only the resulting columns
        calc_type: string, default "reserve_results"
            String to indicate which type of calculation should be made:
            Implemented so far: ("reserve_results", "island_results",
                "trader_results", "offer_results", "branch_results",
                "bus_results")

        Returns
        -------
        combined: DataFrame
            A DataFrame of the combined results including the differences

        """

        indice_dict = {"trader_results": ["Date", "Trader"],
                       "reserve_results": ["DateTime", "Island"],
                       "island_results": ["DateTime", "Island"],
                       "offer_results": ["DateTime", "Offer"],
                       "branch_results": ["DateTime", "Branch", "FromBus", "ToBus"],
                       "bus_results": ["DateTime", "Bus"]
                       }

        if calc_type == "reserve_results":
            self.reserve_procurement(overwrite_results=True)
            other.reserve_procurement(overwrite_results=True)

        # Use dictionaries to make these calculations general purpose
        indices = indice_dict[calc_type]
        left = self.__dict__[calc_type].copy()
        right = other.__dict__[calc_type].copy()

        col_names = left.columns.tolist()
        compare_columns = [x for x in col_names if self._invmatcher(x,indices)]

        left.rename(columns={x: ' '.join([x, left_name]) for
                    x in compare_columns}, inplace=True)

        right.rename(columns={x: ' '.join([x, right_name]) for
                    x in compare_columns}, inplace=True)

        combined = left.merge(right, left_on=indices, right_on=indices)

        for col in compare_columns:
            self._differential(combined, col, left_name=left_name,
                    right_name=right_name, diff_name=diff_name,
                    method=method)

        if diff_only:
            cols = indices + [x for x in combined.columns if diff_name in x]
            combined = combined[cols].copy()

        return combined



    def _differential(self, df, column, left_name=None, right_name=None,
                      diff_name=None, method="Subtract"):
        """ Calculate the differentials between columns in a DataFrame

        Parameters
        ----------
        df: DataFrame
            The Merged DataFrame to work on
        column: string,
            The column name to apply the differential to
        left_name: string, default None
            The name which has been applied to the columns of the left df
        right_name: string, default None
            The name which has been applied to the columns of the right df
        diff_name: string, default None
            The name to call the result
        method: string ("Subtract", "Add")
            What method to apply

        Returns
        -------
        df: DataFrame
            The DataFrame with the differential calculated

        """

        left = " ".join([column, left_name])
        right = " ".join([column, right_name])
        diff = " ".join([column, diff_name])

        if method == 'Subtract':
            df[diff] = df[left] - df[right]

        if method == "Add":
            df[diff] = df[left] + df[right]

        return df


    def _matcher(self, a, p):
        for b in p:
            if b not in a:
                return False
        return True


    def _invmatcher(self, a, p):
        for b in p:
            if b in a:
                return False
        return True


    def _apply_time_filters(self, df, DateTime="DateTime", period=False,
                           day=False, month=False, year=False, inplace=False,
                           month_year=False, dayofyear=False):
        """ Apply a number of time aggregations to the DataFrame for latter
        groupby operations. All arguments are initially set to False to improve
        speed on some of the aggregation queries. Do not ish to aggregate
        more than necessary for a result which will be discarded

        Parameters
        ----------
        df: DataFrame
            DataFrame to apply the time aggregations to
        DateTime: string, default "DateTime"
            column name of the Datetime objects in the DataFrame
        period: bool, default False
            Apply the Trading Period Aggregations
        day: bool, default False
            Apply date aggregations
        month: bool, default False
            Apply a Month aggregation
        year: bool, default False
            Apply a year filter
        month_year: bool, default False
            Filter by Month and Year, defaults to first day of month
        dayofyear: bool, default False
            Filter by day of the year, e.g. Day 323 of the year
        inplace: bool, default False
            Modify the current DataFrame in place if True, else make a copy

        Returns
        -------
        df: DataFrame
            A DataFrame containing the original data as well as the user
            defined filters as desired
        """

        if not inplace:
            df = df.copy()

        df[DateTime] = pd.to_datetime(df[DateTime])

        if day:
            df["Day"] = df[DateTime].apply(lambda x: x.date())

        if month:
            df["Month"] = df[DateTime].apply(lambda x: x.month)

        if year:
            df["Year"] = df[DateTime].apply(lambda x: x.year)

        if month_year:
            df["Month_Year"] = df[DateTime].apply(lambda x: datetime.datetime(x.year, x.month, 1))

        if dayofyear:
            df["Day_Of_Year"] = df[DateTime].apply(lambda x: x.dayofyear)

        if period:
            df["Period"] = df[DateTime].apply(lambda x: x.hour * 2 + 1 + x.minute / 30)

        return df


    def _load_data(self, island=False, summary=False, system=False,
        bus=False, reserve=False, trader=False, offer=False, branch=False,
        node=False):
        """
        Load all of the vSPD data from the given folder.
        If possible pass the folder as an absolute path to minimise issues.
        It is called once on initiation of the class

        Parameters
        ----------
        self.folder : str
            The folder which contains the vSPD results to assess

        Returns
        -------
        self.island_results: DataFrame
        self.summary_results: DataFrame
        self.system_results: DataFrame
        self.bus_results: DataFrame
        self.reserve_results: DataFrame
        self.trader_results: DataFrame
        self.offer_results: DataFrame
        self.branch_results: DataFrame
        """

        folder_contents = glob.glob(os.path.join(self.folder, '*.csv'))
        folder_dict = {os.path.basename(v).split('_')[1]: v for v in folder_contents}

        # Load the data
        if island:
            self.island_results = pd.read_csv(folder_dict["IslandResults"])
        if summary:
            self.summary_results = pd.read_csv(folder_dict["SummaryResults"])
        if system:
            self.system_results = pd.read_csv(folder_dict["SystemResults.csv"])
        if bus:
            self.bus_results = pd.read_csv(folder_dict["BusResults"])
        if reserve:
            self.reserve_results = pd.read_csv(folder_dict["ReserveResults"])
        if trader:
            self.trader_results = pd.read_csv(folder_dict["TraderResults.csv"])
        if node:
            self.node_results = pd.read_csv(folder_dict["NodeResults"])
        if offer:
            self.offer_results = pd.read_csv(folder_dict["OfferResults"])
        if branch:
            self.branch_results = pd.read_csv(folder_dict["BranchResults"])

    def _map_nodes(self, df, map_frame=None, left_on=None, right_on="Node"):
        """

        """

        if not map_frame:
            map_frame = pd.read_csv(CONFIG['map-location'])
            map_frame = map_frame[["Node", "Region", "Island Name", "Generation Type"]]

        map_df = df.merge(map_frame, left_on=left_on, right_on=right_on)
        return map_df



def setup_vspd():
    folder = '/home/nigel/data/Pole_Three_Sample_Data'

    CFact = vSPUD_Factory(folder, "Control")
    OFact = vSPUD_Factory(folder, "Override")

    spud1 = CFact.load_results(island_results=True, summary_results=True,
                system_results=True, bus_results=True, reserve_results=True,
                trader_results=True, offer_results=True, branch_results=True)

    spud2 = OFact.load_results(island_results=True, summary_results=True,
                system_results=True, bus_results=True, reserve_results=True,
                trader_results=True, offer_results=True, branch_results=True)

    return spud1, spud2

if __name__ == '__main__':
    pass
