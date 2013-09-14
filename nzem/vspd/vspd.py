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
import matplotlib.pyplot as plt

# Import nzem
import nzem

# Load the plotting styles
PLOT_STYLES = nzem.plotting.styles.colour_schemes

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
                files) in os.walk(master_folder) if files and
                self._match('.csv', files)]
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

        folders = [dire for (dire, subdir,
                files) in os.walk(self.master_folder) if files and
                self._match('.csv', files)]
        subfolders = [x for x in folders if self._matcher(x, patterns)]
        self.sub_folders = subfolders


    def _matcher(self, a, p):
        for b in p:
            if b not in a:
                return False
        return True

    def _match(self, a, p):
        for b in p:
            if a in b:
                return True
        return False


    def load_all(self, pattern=None):
        """ Convenience wrapper to load the entirety of data from a folder

        Parameters
        ----------
        self: Must have had
        pattern: tuple, default None
            Optional apply a pattern

        Returns
        -------
        vSPUD: class
            A vSPUD object with all data fully loaded

        """

        if isinstance(pattern, tuple):
            self.match_pattern(pattern)

        return self.load_results(island_results=True, summary_results=True,
                system_results=True, bus_results=True, reserve_results=True,
                trader_results=True, offer_results=True, branch_results=True)



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

    def dispatch_report(self, time_aggregation="DateTime",
                        location_aggregation="Island Name",
                        company_aggregation=False,
                        generation_aggregation=False,
                        agg_func=np.sum):
        """ Construct a dispatch report based upon the Offer DataFrame,
        Can perform a variety of aggregations and grouping.

        Parameters
        ----------
        time_aggregation: string, default "DateTime"
            Column name of the time aggregation to be applied options include
            ("Period", "Day", "Month", "Year", "Month_Year", "Day_Of_Year")
        location_aggregation: string, default "Island Name"
            Column name of the location aggregation to be applied options
            ("Island Name", "Region", "Offers")
        company_aggregation: bool, default False
            Not currently implemented, will be what column name of the company
            to aggregate by
        generation_aggregation: bool, default False
            Whether to aggregate by generation type or not
        agg_func: function
            Aggregation function to apply in the report

        Returns
        -------
        report: DataFrame
            A DataFrame containing the specified report

        """

        # If company aggregation applied set the column name
        # Not implemented currently
        if company_aggregation:
            company_aggregation = False

        # If generation aggregation set the column name
        if generation_aggregation:
            generation_aggregation = "Generation Type"

        aggregations = (time_aggregation, location_aggregation,
                        company_aggregation, generation_aggregation)

        kargs = self._time_keywords(time_aggregation)

        # Map the Offers
        mapoffers = self.map_dispatch()
        timeoffers = self._apply_time_filters(mapoffers, **kargs)

        # Construct the group by columns
        group_col = [x for x in aggregations if x]

        # Construct the report
        return timeoffers.groupby(group_col).aggregate(agg_func)


    def dispatch_table(self, other, report_unit="GWh",
                        latex_fName=None, force_int=True,
                        report_columns=("Generation", "FIR", "SIR"),
                        time_aggregation="Year",
                        location_aggregation="Island Name",
                        company_aggregation=False,
                        generation_aggregation=True,
                        agg_func=np.sum,left_name="Control",
                        right_name="Override", diff_name="Diff"):
        """ Construct a dispatch table and optionally output this to
        a LaTeX file for inclusion in a document.

        Currently a few issues with formatting the LaTeX tables.
        Need to work on this. Most likely with the formatters?
        Not sure. Currently option to force integer results exist.

        Parameters
        ----------
        self: class vSPUD
            A vSPUD class with dispatch results present
        other: class vSPUD
            Another vSPUD class with dispatch results present
        report_unit: string, default "GWh"
            What unit the results should be reported in.
        latex_fName: string, default None
            Optional, save the result to a LaTeX file
        force_int: bool, default True
            Force the output to be as integers, not floats
        report_columns: tuple, default ("Generation", "FIR", "SIR")
            What metrics to compute, may be a subset of the default tuple
            only.
        time_aggregation: string, default "DateTime"
            Column name of the time aggregation to be applied options include
            ("Period", "Day", "Month", "Year", "Month_Year", "Day_Of_Year")
        location_aggregation: string, default "Island Name"
            Column name of the location aggregation to be applied options
            ("Island Name", "Region", "Offers")
        company_aggregation: bool, default False
            Not currently implemented, will be what column name of the company
            to aggregate by
        generation_aggregation: bool, default False
            Whether to aggregate by generation type or not
        agg_func: function
            Aggregation function to apply in the report
        left_name: string, default "Control"
            Identifying name to apply to the original vSPUD object
        right_name: string, default "Override"
            Identifying name to apply to the second vSPUD object
        diff_name: string, default "Difference"
            Identifying name to apply to the difference

        Returns
        -------
        combined: DataFrame
            DataFrame with the tabulated report data present
        """

        # Multiplication factors to scale from the given MW values
        report_scale = {"kWh": 500, "MWh": 0.5, "GWh": 0.0005, "TWh": 0.000005}

        # Construct the dispatch reports
        init_dispatch = self.dispatch_report(
                        time_aggregation=time_aggregation,
                        location_aggregation=location_aggregation,
                        company_aggregation=company_aggregation,
                        generation_aggregation=generation_aggregation,
                        agg_func=agg_func)

        other_dispatch = other.dispatch_report(
                        time_aggregation=time_aggregation,
                        location_aggregation=location_aggregation,
                        company_aggregation=company_aggregation,
                        generation_aggregation=generation_aggregation,
                        agg_func=agg_func)


        # Scale the values
        init_dispatch = init_dispatch * report_scale[report_unit]
        other_dispatch = other_dispatch * report_scale[report_unit]

        compare_columns = init_dispatch.columns.tolist()

        # Rename the columns
        init_dispatch.rename(columns={x: " ".join([x, left_name]) for x in init_dispatch.columns}, inplace=True)

        other_dispatch.rename(columns={x: " ".join([x, right_name]) for x in other_dispatch.columns}, inplace=True)

        # Merge the DataFrames
        combined = init_dispatch.merge(other_dispatch, left_index=True,
                                                       right_index=True)

        # Calculate the differences

        for col in compare_columns:
            self._differential(combined, col, left_name=left_name,
                    right_name=right_name, diff_name=diff_name,
                    method="Subtract")

        if force_int:
            combined = combined.astype(np.int64)

        # Report Columns
        report_col = [x for x in combined.columns if self._matchiter(x,
                                                report_columns)]

        combined = combined[report_col].copy()

        # Change the reporting unit
        combined.rename(columns={x: x.replace("MW", report_unit) for x in combined.columns if "MW" in x}, inplace=True)

        # Get rid of the multi index and return as DataFrame
        combined = self._mindex_to_col(combined)

        if latex_fName:
            combined.to_latex(latex_fName)

        return combined

    def _mindex_to_col(self, df, int_index=True):
        """ Convert a multi index back to DataFrame columns
        Will preserve the ordering of the multi index as column indices

        Parameters
        ----------
        df: DataFrame
            A DataFrame which is named and has a multi index
        int_index: bool, default True
            Replace the current index with an integer index

        Returns
        -------
        df: DataFrame
            DataFrame with a multi index as columns

        """

        index_array = np.array(df.index.tolist())
        for i, name in enumerate(df.index.names):
            df.insert(i, name, index_array[:,i])

        if int_index:
            df.index = np.arange(len(df))

        return df


    def price_series(self, time_aggregation="Month_Year", agg_func=np.mean):
        """ Construct an aggregated price series according to the aggregation
        metric and function specified

        Parameters
        ----------
        self:
        time_aggregation: string, default "Month_Year"
            The time aggregation to be applied
        agg_func: func
            The aggregation function to apply

        Returns
        -------
        price_report: DataFrame
            An aggregated price report of time series data

        """
        # Create a report
        price_report = self.price_report()

        if time_aggregation:
            karg_dict = self._time_keywords(time_aggregation)
            price_report = self._apply_time_filters(price_report, **karg_dict)
            # Aggregate
            price_report = price_report.groupby(time_aggregation
                                               ).aggregate(np.mean)

        return price_report


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
        apply_time: string, default False, optional
            Apply the time aggregations, must be a recognizable string.
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


        fir_cols = ["FIR Reqd (MW)", "FIR Price ($/MW)"]
        sir_cols = ["SIR Reqd (MW)" ,"SIR Price ($/MW)"]

        res_results["FIR Procurement ($)"] = res_results[fir_cols].product(axis=1)
        res_results["SIR Procurement ($)"] = res_results[sir_cols].product(axis=1)

        if apply_time:
            karg_dict = self._time_keywords(apply_time)
            res_results = self._apply_time_filters(res_results, **karg_dict)

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

    def _matchiter(self, a, p):
        for b in p:
            if b in a:
                return True
        return False


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
            my = lambda x: datetime.datetime(x.year, x.month, 1)
            df["Month_Year"] = df[DateTime].apply(my)

        if dayofyear:
            df["Day_Of_Year"] = df[DateTime].apply(lambda x: x.dayofyear)

        if period:
            pc = lambda x: x.hour * 2 + 1 + x.minute / 30
            df["Period"] = df[DateTime].apply(pc)

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
        """ Map a DataFrame by its nodal location to a range of metadata
        Can either pass a DataFrame of metadata to use, or rely upon the
        built in set provided

        Parameters
        ----------
        df: DataFrame
            The DataFrame to be mapped
        map_frame: DataFrame, bool, default None
            Optional, the mapping frame to use
        left_on: string, default None
            What column to merge the meta data on
        right_on: string, default None
            What column in the mapping frame to merge by

        Returns
        -------
        map_df: DataFrame
            A DataFrame consisting of the original DataFrame plus the
            additional Metadata merged in.

        """

        if not isinstance(map_frame, pd.DataFrame):
            map_frame = pd.read_csv(CONFIG['map-location'])
            map_frame = map_frame[["Node", "Region", "Island Name", "Generation Type"]]

        return df.merge(map_frame, left_on=left_on, right_on=right_on)

    def _time_keywords(self, x=None):
        time_dict = {"Month_Year": {'month_year': True},
                     "Period": {"period": True},
                     "Day": {'day': True},
                     "Month": {'month': True},
                     "Year": {'year': True},
                     "Day_Of_Year": {'day_of_year': True}
                     }

        if isinstance(x, str):
            return time_dict[x]

        elif isinstance(x, tuple) or isinstance(x, list):
            return {k: v for i in x for k, v in time_dict[i].iteritems()}

    # PLOT COMMANDS

    def frequency_plot(self, axes, colour_dict='greyscale_bar',
                       freq_name=None, comp='orig'):
        """ Create a frequency distribution for a reserve type

        Parameters
        ----------
        axes:
        colour_dict:
        freq_name:
        comp:

        Returns
        -------
        axes: Matplotlib axes with plotted data

        """
        styling = PLOT_STYLES[colour_dict]

        st_name = '_'.join([freq_name.lower(), comp])
        axes.hist(self.island_results[freq_name], bins=50, **styling[st_name])

        axes.set_xlabel(name)
        axes.set_ylabel('Frequency')

        return axes

    def mixed_price_plot(self, other, colour_dict='greyscale_line',
                           time_aggregation='Month_Year', agg_func=np.mean):
        """ Create a three part plot of energy and reserve prices

        Parameters
        ----------
        self: Reserves prices for instance one
        other: Reserve Prices for the second comparison object
        colour_dict: What colour dictionary to use
        time_aggregation: What time aggregation to use
        agg_func: What aggregation function to use on the prices

        Returns
        -------
        fig, axes: A Matplotlib plot object

        """

        # Create two price series to cut down on duplication of effort
        self_prices = self.price_series(time_aggregation=time_aggregation,
                                        agg_func=agg_func)

        other_prices = other.price_series(time_aggregation=time_aggregation,
                                        agg_func=agg_func)

        fig, axes = plt.subplots(3,1, figsize=(12,9))

        self.energy_price_plot(axes[0], prices=self_prices,
                               colour_dict=colour_dict,
                               time_aggregation=time_aggregation,
                               agg_func=agg_func, comp='orig')

        other.energy_price_plot(axes[0], prices=other_prices,
                               colour_dict=colour_dict,
                               time_aggregation=time_aggregation,
                               agg_func=agg_func, comp='alt')

        self.reserve_price_plot(axes[1], prices=self_prices,
                               colour_dict=colour_dict,
                               time_aggregation=time_aggregation,
                               agg_func=agg_func, comp='orig',
                               island="NI")

        other.reserve_price_plot(axes[1], prices=other_prices,
                               colour_dict=colour_dict,
                               time_aggregation=time_aggregation,
                               agg_func=agg_func, comp='alt',
                               island="NI")

        self.reserve_price_plot(axes[2], prices=self_prices,
                               colour_dict=colour_dict,
                               time_aggregation=time_aggregation,
                               agg_func=agg_func, comp='orig',
                               island="SI")

        other.reserve_price_plot(axes[2], prices=other_prices,
                               colour_dict=colour_dict,
                               time_aggregation=time_aggregation,
                               agg_func=agg_func, comp='alt',
                               island="SI")

        return fig, axes

    def energy_price_plot(self, axes, prices=None,
                          time_aggregation="Month_Year",
                          colour_dict='greyscale_line', comp='orig',
                          agg_func=np.mean, label_dict="Pole3"):
        """ Plot the Energy Prices on a given Axes
        Ideal is to pass an aggregation and then let the function take
        care of the rest.

        Parameters
        ----------
        axes: The matplotlib axes to plot the data on
        prices: Optional, pass a Price report with aggregations.
        time_aggregation: Aggregation to accomplish
        colour_dict: The colour styles to use
        comp: Original or Alternative colour styles
        agg_func: Aggregation function to apply

        Returns
        -------
        axes: Plotted energy price series plot

        """
        # Grab the colour directory
        styling = PLOT_STYLES[colour_dict]
        labels = self._label_dict(label_dict)

        # Get the Prices
        if not prices:
            prices = self.price_series(time_aggregation=time_aggregation,
                                   agg_func=agg_func)
        haywards = prices["NI ReferencePrice ($/MWh)"]
        benmore = prices["SI ReferencePrice ($/MWh)"]

        hps = '_'.join(['haywards_price', comp])
        bps = '_'.join(['benmore_price', comp])

        hay_label = " ".join(["Haywards Price", labels[comp]])
        ben_label = " ".join(["Benmore Price", labels[comp]])

        # Plot the Data
        haywards.plot(ax=axes, label=hay_label, **styling[hps])
        benmore.plot(ax=axes, label=ben_label, **styling[bps])

        # Handle the Axes Labelling
        axes.set_ylabel('Energy Price ($/MWh)')
        axes.set_xlabel('')

        axes.legend(loc='upper right')

        return axes

    def reserve_price_plot(self, axes, prices=None,
                           time_aggregation="Month_Year",
                           island="NI", colour_dict="greyscale_line",
                           comp="orig", agg_func=np.mean,
                           label_dict="Pole3"):
        """ Create a Reserve Price plot of both FIR and SIR for a
        particular island

        Parameters
        ----------
        axes: The matplotlib axes to plot the data on
        prices: Optional, pass a price series report
        time_aggregation: Aggregation to accomplish
        colour_dict: The colour styles to use
        comp: Original or Alternative colour styles
        agg_func: Aggregation function to apply
        island: What islands reserve prices to plot

        Returns
        -------
        axes: Plotted energy price series plot

        """

        styling = PLOT_STYLES[colour_dict]
        labels = self._label_dict(label_dict)

        # Get the prices
        if not prices:
            prices = self.price_series(time_aggregation=time_aggregation,
                                   agg_func=np.mean)

        # Name the variables
        fir_name = " ".join([island, "FIR Price ($/MWh)"])
        sir_name = " ".join([island, "SIR Price ($/MWh)"])

        fps = '_'.join(['fir_price', comp])
        sps = '_'.join(['sir_price', comp])

        fir_label = " ".join(["FIR Price", labels[comp]])
        sir_label = " ".join(["SIR Price", labels[comp]])

        # Plot the Prices
        prices[fir_name].plot(ax=axes, label=fir_label, **styling[fps])
        prices[sir_name].plot(ax=axes, label=sir_label, **styling[sps])

        # Axes Labelling
        ylab = " ".join([island, "Reserve Prices ($/MWh)"])
        axes.set_ylabel(ylab)
        axes.set_xlabel('')

        axes.legend(loc='upper right')

        return axes

    def _label_dict(self, label):
        if label =="Pole3":
            return {"alt": "w/ Pole Three", "orig": "no Pole Three"}
        else:
            return None

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
