from nzem.utilities.masks import apply_masks, apply_series_masks
from nzem.utilities.utilities import *
from nzem.data_handling.data_import import *
from nzem.analysis.offer_curves import *
from nzem.analysis.constraints import *
from nzem.analysis.hydrology import *
from nzem.utilities.ts_aggregation import *
from nzem.utilities.aggregation import *
from nzem.data_handling.master_set import *
import pandas as pd


# Modifications to be made
pd.Dataframe = nzem.apply_masks()
pd.Series = nzem.apply_series_masks()
