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

class vSPUD(object):
    """docstring for vSPUD"""
    def __init__(self, folder):
        super(vSPUD, self).__init__()

        self.folder = folder
        self._load_data()

    def _load_data(self):
        """
        Load all of the vSPD data from the given folder.
        If possible pass the folder as an absolute path to minimise issues

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
        self.island_results = pd.read_csv(folder_dict["IslandResults"], parse_dates=True)
        self.summary_results = pd.read_csv(folder_dict["SummaryResults"], parse_dates=True)
        self.system_results = pd.read_csv(folder_dict["SystemResults"], parse_dates=True)
        self.bus_results = pd.read_csv(folder_dict["BusResults"], parse_dates=True)
        self.reserve_results = pd.read_csv(folder_dict["ReserveResults"], parse_dates=True)
        self.trader_results = pd.read_csv(folder_dict["TraderResults"], parse_dates=True)
        self.node_results = pd.read_csv(folder_dict["NodeResults"], parse_dates=True)
        self.offer_results = pd.read_csv(folder_dict["OfferResults"], parse_dates=True)
        self.branch_results = pd.read_csv(folder_dict["BranchResults"], parse_dates=True)



if __name__ == '__main__':
    pass
