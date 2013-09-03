""" Script to hit the Electricity WITS site and download offer files and data
for particular dates and types. It is designed to be stand alone and automated
if at all possible.

Will likely develop a CRON job for it to hit and download to a particular
location as raw files, not as a Database. A Database module may be introduced
at a later date

Author: Nigel Cleland
Date: 30th July 2013
License: MIT

This script provides the WitsScraper class which is a general purpose scraper
intended to make interfacing with the WITS free to air site much nicer.
"""

# Standard Library
import os
import urlparse
import subprocess
import sys
import datetime
from collections import defaultdict
from dateutil.parser import parse

# Non C Dependency
import requests as rq
import simplejson as json
from bs4 import BeautifulSoup

# C Depencency
import pandas as pd

# Load the master CONFIG json file
try:
    CONFIG = json.load(open(os.path.join(
        os.path.expanduser('~/python/nzem/nzem/_static'), 'config.json')))
    gnash_path = CONFIG['gnash-path']
except:
    print "CONFIG File does not exist"


class WitsScraper:

    def __init__(self, scrape_offers=True, scrape_demand=True):
        print "Initialising WitsScraper:"
        if scrape_offers:
            print "Scraping Offer Files"
            self.current_files, self.historic_files = self._scrape_offers()
            print "Offer Files Successfully Scraped"
        if scrape_demand:
            print "Scraping Demand Files"
            print "Demand Files Sucessfully Scraped"


    def _scrape_offers(self, current=True, historic=True, sortfiles=True):
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
            current_files = self._scrape_offer(CONFIG['wits-offer-current'])

        if historic:
            historic_files = self._scrape_offer(CONFIG['wits-offer-historic'])

        if sortfiles:
            if current:
                current_files = self._sort_files(current_files)
            if historic_files:
                historic_files = self._sort_files(historic_files)

        return (current_files, historic_files)


    def _scrape_offer(self, path):
        """
        Non-exposed method to scrape a particular path, it assumes that the site
        is the WITS free to air site and will preprend this to the requests query.

        Notes:
        ------
        This function was exhibiting strange behaviour and not returning all of
        the links when implemented in a simple fashion. To overcome this I've
        split the requests message (r.text) to be parsed into chunks which are
        then fed to the link finder. This isn't perfect and could likely be
        implemented in a far nicer fashion, however it suffices for now.

        Parameters
        ----------
        path: The path to be scraped, either current or historic offers typically

        Returns
        -------
        offer_files: A list of all of the offer files which could be scraped from the
                     particular path.
        """
        r = rq.get(os.path.join(CONFIG['wits-site'], path))
        chunky_soup = self._chunk_soup(r.text)
        soups = (BeautifulSoup(chunk) for chunk in chunky_soup)
        chunky_links = (soup_chunk.find_all('a') for soup_chunk in soups)
        links = self._flatten(chunky_links)
        offer_files = self._link_yield(links)
        return offer_files


    def _sort_files(self, files):
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

    def _download_file(self, url, directory):
        """
        Non-exposed method to download a URL file to a particular location

        Parameters
        ----------
        url: The particular URL to be downloaded
        directory: The directory where the file will be downloaded to

        Returns
        -------
        loc: The absolute path of the file destination
        """

        local_name = os.path.split(url)[1]
        loc = os.path.join(directory, local_name)
        r = rq.get(url, stream=True)
        with open(loc, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
        return loc


    def _extract_file(self, file_name, delete_original=True, output=False):
        """
        Non-exposed method to extract a compressed file in the same location
        as the file itself, not the present path. Optional flags to delete
        the compressed file and return output if desired

        Parameters
        ----------
        file_name: The file name to be extracted

        Returns
        -------
        output_name: The name of the extracted file created by 7z
        """

        directory = os.path.split(file_name)[0]
        output_filename = os.path.splitext(os.path.split(file_name)[1])[0]
        outdir = '-o%s' % directory
        output_name = os.path.join(directory, output_filename)
        s = subprocess.call(['7z', 'e', '-y', outdir, file_name], stderr=subprocess.STDOUT,
                        stdout=subprocess.PIPE)

        if delete_original:
            os.remove(file_name)

        if output:
            print "Successfully extracted %s to %s" % (file_name,
                                                       output_name)
            if delete_original:
                print "Successfully deleted %s" % file_name

        return output_name


    def _filter_offer_dates(self, product=False, begin_date=None, end_date=None):
        """ Find the file(s) which contain the beginning and end date within them
        Return the file for a specific product as required, else return
        a dictionary with lists containing the output.

        Parameters
        ----------
        product: The product to be filtered (if any)
        begin_date: The beginning date to find files for
        end_date: The end date to find files for

        Returns
        -------
        search_results: The results of the search through the offer files
        """

        begin_date = self._parse_to_datetime(begin_date)
        end_date = self._parse_to_datetime(end_date)

        begin_str = begin_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')

        # Filter by product if needed
        current = list(self._yield_product(self.current_files, product=product))
        historic = list(self._yield_product(self.historic_files, product=product))



        # Try the current files first
        if self._match_true(begin_str, current) and self._match_true(end_str, current):
            dates = [d.strftime("%Y%m%d") for d in pd.date_range(begin_date,
                                                                 end_date)]

            search_results = [self._match_iterables(item, current)
                              for item in dates]

        # See if it partially matches historic, partially matches current
        elif self._match_true(end_str, current) and not self._match_true(begin_str, current):

            daily_dates = [d.strftime("%Y%m%d") for d in pd.date_range(begin_date,
                                                                 end_date)]
            monthly_dates = [d.strftime("%Y%m") for d in pd.date_range(
                                                    begin_date,
                                                    end_date + datetime.timedelta(days=31),
                                                    freq="M", normalize=True)]

            daily_results = [self._match_iterables(item, current)
                              for item in daily_dates]
            monthly_results = [self._match_iterables(item, historic) for item in
                                monthly_dates]

            search_results = daily_results + monthly_results

        # Scrape the historic files
        elif begin_str not in current and end_str not in current:
            month_dates = [d.strftime("%Y%m") for d in pd.date_range(
                                                    begin_date,
                                                    end_date + datetime.timedelta(days=31),
                                                    freq="M", normalize=True)]

            search_results = [self._match_iterables(item, historic) for item in
                                month_dates]

        # Return the search results, save the last search to an internal
        # attribute for safe keeping
        search_results = filter(lambda x: x != None, search_results)
        self.search_results = search_results
        return search_results

    def _download_search(self, directory, search_results=None, extract=True):

        if search_results == None:
            try:
                search_results = self.search_results
            except:
                raise ValueError('You must have either completed a recent search\
                                  or pass search results to this function')

        self._ensure_directory(directory)
        print "Attempting to downloads files to %s" % directory
        for result in search_results:
            try:
                location = self._download_file(result, directory)
                if extract:
                    output_name = self._extract_file(location)
            except:
                url = urlparse.urljoin(CONFIG['wits-site'], result)
                location = self._download_file(url, directory)
                if extract:
                    output_name = self._extract_file(location)
        print "Downloads completed"


    #### Assistance and helper functions

    def _match_iterables(self, item, iterable):
        """ Return the string in the iterable which matches the item """
        for it in iterable:
            if item in it:
                return it

    def _match_true(self, item, iterable):
        """ Return true if item matches any part of the items in the iterable """
        for it in iterable:
            if item in it:
                return True
        return False


    def _parse_to_datetime(self, dt):
        """ Simple function to parse the datetime if it is not already a
        datetime
        """
        if type(dt) != datetime.datetime:
            dt = parse(dt)
        return dt

    def _yield_product(self, d, product=False):
        """ Yield a flattened list of items from a nested dictionary """
        if product:
            for item in d[product]:
                yield item
        else:
            for key in d.keys():
                for item in d[key]:
                    yield item

    def _chunk_soup(self, rtext, chunk_size=1000):
        """ Return a number of requests text chunks, this is to ensure
        that the scraper does not error before reaching the end.
        Quite a strange bug really...

        Parameters
        ----------
        rtext: The requests text to be chunked (not this is typically a string)
        chunk_size: The size of each chunk
        """
        while rtext:
            if len(rtext) >= chunk_size:
                chunk, rtext = rtext[:chunk_size], rtext[chunk_size:]
                yield chunk
            else:
                chunk = rtext
                yield chunk
                rtext = None


    def _flatten(self, lol):
        """ Flattens a list of lists (lol) and returns a generator """
        return (item for sublist in lol for item in sublist)


    def _link_yield(self, links):
        """ Yield all csv links from a list of BeautifulSoup links """
        for link in links:
            href = link.get('href')
            if type(href) == str:
                if '.csv' in href:
                    yield href

    def _ensure_directory(self, directory):
        """ Check to see if a directory exists, if not create it """
        if not os.path.exists(directory):
            os.mkdir(directory)
        return None


















