"""
Module to make working with vSPD data a lot simpler and easier
"""

# Standard Library Imports
import sys
import os
import datetime
import glob

# C Library Imports

import pandas as pd
import numpy as np

# Import nzem

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
                files) in os.walk(master_folder) if files]
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


    def _apply_time_filters(self, df, DateTime="DateTime", period=True,
                           day=True, month=True, year=True, inplace=True,
                           month_year=True, dayofyear=True):
        """ Apply a number of time aggregations to the DataFrame for latter
        groupby operations

        Parameters
        ----------

        Returns
        -------

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

if __name__ == '__main__':
    pass
