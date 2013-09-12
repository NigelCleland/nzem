""" Plotting styles for the NZEM module.

Provides a Black and white colour scheme for publication plots.
For a colour scheme with more "vibrant" colours utilise the
coloured file instead.

"""

import matplotlib as mpl


line_dict = {'haywards_price': {'c': 'black', 'alpha': 0.7, 'linestyle': '-'},
             'benmore_price': {'c': 'black', 'alpha': 0.3, 'linestyle': '-'},
             'fir_price': {'c': 'black', 'alpha': 0.7, 'linestyle': '--'},
             'sir_price': {'c': 'black', 'alpha': 0.3, 'linestyle': '--'},
             'ni_reserve_price': {'c': 'black', 'alpha': 0.7,
                                  'linestyle': '.-'},
             'si_reserve_price': {'c': 'black', 'alpha': 0.3,
                                  'linestyle': '.-'}
             }
