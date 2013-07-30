""" Script to hit the Electricity WITS site and download offer files and data
for particular dates and types. It is designed to be stand alone and automated
if at all possible.

Will likely develop a CRON job for it to hit and download to a particular
location as raw files, not as a Database. A Database module may be introduced
at a later date

Author: Nigel Cleland
Date: 30th July 2013
License: MIT
"""

# Module Imports
import pandas as pd
import requests as rq
import simplejson as json
import os

# Secondary imports
from bs4 import BeautifulSoup
from collections import defaultdict

# Load the master CONFIG json file
CONFIG = json.load(open(os.path.join(os.path.expanduser('~/python/nzem/nzem'),
                         'config.json')))

def scrape_offers(current=True, historic=True, sortfiles=True):
    """ Scrape all of the offers from either the current and historic offers,
    Sort these into a dictionary to group like with like.

    Parameters
    ----------
    current: Scrape the current daily files off the website
    historic: Scrape the historic monthly files off the website
    sortfiles: Apply the dictionary binning procedure

    Returns
    -------
    current_files: The current files, either sorted or unsorted
    historic_files: The historic files, either sorted or unsorted
    """
    current_files = None
    historic_files = None

    if current:
        current_files = _scrape_offer(CONFIG['wits-offer-current'])

    if historic:
        historic_files = _scrape_offer(CONFIG['wits-offer-historic'])

    if sortfiles:
        if current:
            current_files = _sort_files(current_files)
        if historic_files:
            historic_files = _sort_files(historic_files)

    return (current_files, historic_files)


def _scrape_offer(path):
    """
    Non-exposed method to scrape a particular path, it assumes that the site
    is the WITS free to air site and will preprend this to the requests query.

    Parameters
    ----------
    path: The path to be scraped, either current or historic offers typically

    Returns
    -------
    offer_files: A list of all of the offer files which could be scraped from the
                 particular path.
    """
    r = rq.get(os.path.join(CONFIG['wits-site'], path))
    soup = BeautifulSoup(r.text)

    links = soup.find_all('a')
    offer_files = [link.get('href') for link in links if '.csv' in link.get('href')]
    return offer_files

def _sort_files(files):
    """
    Non-exposed method to filter the current and historic files into bins depending
    upon their type.

    Parameters
    ----------
    files: The files to be sorted

    Returns
    -------
    filelisting: The sorted files according to the WITS offer types as scraped
                 from the configuration file
    """

    filelisting = defaultdict(list)

    for f in files:
        path, name = os.path.split(f)
        for ty in CONFIG['wits-offer-types']:
            if name.startswith(ty):
                filelisting[ty].append(f)

    return filelisting





    