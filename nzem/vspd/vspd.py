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
    def __init__(self, master_folder, pattern=None):
        super(vSPUD_Factory, self).__init__()
        self.master_folder = master_folder
        self.pattern = pattern
        self.sub_folders = glob.glob(os.path.join(master_folder, '*'))
        self.sub_folders = [x for x in self.sub_folders if os.path.isdir(x)]
        if pattern:
            self.sub_folders = [x for x in self.sub_folders if pattern in x]


    def load_results(self, island_results=None, summary_results=None,
                system_results=None, bus_results=None, reserve_results=None,
                trader_results=None, offer_results=None, branch_results=None):
        """
        Load all of the reserve results
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
        folder_dict = {os.path.splitext(os.path.basename(v))[0].split('_')[1]: v for v in folder_contents}

        # Load the data
        if island:
            self.island_results = pd.read_csv(folder_dict["IslandResults"], parse_dates=True)
        if summary:
            self.summary_results = pd.read_csv(folder_dict["SummaryResults"], parse_dates=True)
        if system:
            self.system_results = pd.read_csv(folder_dict["SystemResults"], parse_dates=True)
        if bus:
            self.bus_results = pd.read_csv(folder_dict["BusResults"], parse_dates=True)
        if reserve:
            self.reserve_results = pd.read_csv(folder_dict["ReserveResults"], parse_dates=True)
        if trader:
            self.trader_results = pd.read_csv(folder_dict["TraderResults"], parse_dates=True)
        if node:
            self.node_results = pd.read_csv(folder_dict["NodeResults"], parse_dates=True)
        if offer:
            self.offer_results = pd.read_csv(folder_dict["OfferResults"], parse_dates=True)
        if branch:
            self.branch_results = pd.read_csv(folder_dict["BranchResults"], parse_dates=True)



if __name__ == '__main__':
    pass
