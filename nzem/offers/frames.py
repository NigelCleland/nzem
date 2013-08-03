import pandas as pd
import numpy as np

class ILOffer(object):
    """
    ILOffer
    ===========
    
    Wrapper around an IL Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, df):
        super(ILOffer, self).__init__()
        self.df = df

    def __repr__(self):
        return self.df

class PLSROffer(object):
    """
    PLSROffer
    ===========
    
    Wrapper around an PLSR Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, df):
        super(PLSROffer, self).__init__()
        self.df = df

    def __repr__(self):
        return self.df


class EnergyOffer(object):
    """
    EnergyOffer
    ===========
    
    Wrapper around an Energy Offer dataframe which provides a number
    of useful functions in assessing the Energy Offers.
    Is created by passing a pandas DataFrame in the standard WITS
    template and then modificiations are made from there
    """
    def __init__(self, df):
        super(EnergyOffer, self).__init__()
        self.df = df

    def __repr__(self):
        return self.df
