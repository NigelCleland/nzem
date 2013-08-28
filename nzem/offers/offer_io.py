"""
Offer Input Output
"""

# Standard Library
import glob
import os
import datetime
from dateutil.parser import parse

# Non C dependency
import simplejson as json

# C Depencency
import pandas as pd

from nzem import ILOffer, PLSROffer, EnergyOffer

try:
    CONFIG = json.load(open(os.path.join(
        os.path.expanduser('~/python/nzem/nzem/_static'), 'config.json')))
except:
    print "CONFIG File does not exist"

def offer_from_file(begin_date=None, end_date=None, offer_type="IL",
                    file_date_format='%b_%Y'):
    """ Create an Offer DataFrame by searching the appropriate directory
    and loading from a CSV file. Assumes different behaviour depending
    upon what offer_type is passed to the function.

    Configuration data for this script is found in the config.json file

    Parameters
    ----------
    begin_date: string, default None
        The beginning date, forms what range to load files for
    end_date: string, default None
        The end date, forms the end of the range to load files for
    offer_type: string, default "IL"
        The type of offer to load the data for
    file_date_format: string, default "%b_%Y"
        The file date formats of the offer file name


    Returns
    -------
    ILOffer: type ILOffer
        Container around an IL Offer
    PLSROffer: type PLSROffer
        Container around a PLSR Offer
    EnergyOffer: type EnergyOffer
        Container around an Energy Offer DataFrame

    """

    # Set the file_path based upon the offer_type:
    file_path = CONFIG["-".join([offer_type.lower(), 'file-location'])]
    all_files = glob.glob(os.path.join(file_path, '*.csv'))

    if not begin_date and not end_date:
        raise ValueError("You must pass both a begin_date and end_date")


    # Begin filtering the data...
    if not isinstance(begin_date, datetime.datetime):
        begin_date = parse(begin_date)
    if not isinstance(end_date, datetime.datetime):
        end_date = parse(end_date)

    dates = tuple(set([x.strftime(file_date_format) for x in pd.date_range(begin_date, end_date)]))

    unique_files = [x for x in all_files if multi_match(x, dates)]

    # Load the DataFrame
    df = pd.concat((pd.read_csv(x) for x in unique_files), ignore_index=True)

    # Construct an Offer dictionary

    frame_dict = {"IL": ILOffer, "PLSR": PLSROffer, "Energy": EnergyOffer}

    offer = frame_dict[offer_type](df)

    # Filter the dates again, discarding the unneeded data
    offer.filter_dates(begin_date=begin_date, end_date=end_date, horizontal=True, inplace=True, return_df=False)


    return offer

def reserve_offer_from_file(begin_date=None, end_date=None,
                            file_date_format="%b_%Y"):
    """ Import both IL, PLSR and TWDSR data from file and merge the two
    together to reduce the number of redundant steps which need to be taken.

    Parameters
    ----------
    begin_date: string, default None
        The beginning date, forms what range to load files for
    end_date: string, default None
        The end date, forms the end of the range to load files for
    offer_type: string, default "IL"
        The type of offer to load the data for
    file_date_format: string, default "%b_%Y"
        The file date formats of the offer file name

    Returns
    -------
    ReserveOffer: class
        The ReserveOffer container which has stacked reserve offer data

    """

    il_offer = offer_from_file(begin_date=begin_date, end_date=end_date,
            file_date_format=file_date_format, offer_type="IL")

    plsr_offer = offer_from_file(begin_date=begin_date, end_date=end_date,
            file_date_format=file_date_format, offer_type="PLSR")

    return il_offer.merge_stacked_offers(plsr_offer)

def offer_from_wits():
    pass


def offer_from_db():
    pass


def multi_match(item, iterable):
    for it in iterable:
        if it in item:
            return True
    return False


if __name__ == '__main__':
    pass
