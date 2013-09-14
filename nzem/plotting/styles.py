""" Plotting styles for the NZEM module.

Provides a series of colour schemes for consistent plot styling
Because comparison plotting will often occur the major dictionaries
provide at least two styles for each major factor.
E.g. haywards_price will have both _orig and _compare.

Currently Implemented:
----------------------
greyscale: Grey Scheme which uses line types, markets and opacity to
            distinguish between different variables. Intended for black
            and white printing, or publications


To Be Implements
----------------
colour: Utilise different colours to differentiate between options

generic: A generic colour scheme to cycle through as needed

gen_styles: Generic Styles which are utilised for both kinds of plots

"""

import matplotlib as mpl


gs_line_dict = {'haywards_price_orig': {'c': 'black', 'alpha': 0.7,
                                   'linestyle': '-'},
             'benmore_price_orig': {'c': 'black', 'alpha': 0.3,
                                    'linestyle': '-'},
             'fir_price_orig': {'c': 'black', 'alpha': 0.7, 'linestyle': '--'},
             'sir_price_orig': {'c': 'black', 'alpha': 0.3, 'linestyle': '--'},
             'ni_reserve_price_orig': {'c': 'black', 'alpha': 0.7,
                                  'linestyle': '-', 'marker':'.'},
             'si_reserve_price_orig': {'c': 'black', 'alpha': 0.3,
                                  'linestyle': '-', 'marker': '.'},
             'haywards_price_alt': {'c': 'black', 'alpha': 0.7,
                                   'linestyle': '-', 'marker':'.'},
             'benmore_price_alt': {'c': 'black', 'alpha': 0.3,
                                    'linestyle': '-', 'marker':'.'},
             'fir_price_alt': {'c': 'black', 'alpha': 0.7,
                               'linestyle': '--', 'marker': '.'},
             'sir_price_alt': {'c': 'black', 'alpha': 0.3,
                               'linestyle': '.--', 'marker': '.'},
             'ni_reserve_price_alt': {'c': 'black', 'alpha': 0.7,
                                  'linestyle': 'o--'},
             'si_reserve_price_alt': {'c': 'black', 'alpha': 0.3,
                                  'linestyle': 'o--'}
             }

gs_scatter_dict = {

                }

gs_bar_dict = {

}

co_line_dict = {}

co_scatter_dict = {}

# Aggregate the style dictionaries together.
colour_schemes = {'greyscale_line': gs_line_dict,
                  'greyscale_scatter': gs_scatter_dict,
                  'greyscale_bar': gs_bar_dict,
                  'colour_line': co_line_dict,
                  'colour_scatter': co_scatter_dict
                  }


style_dict = {'figure.figsize': (16,9),
              'figure.dpi': 100,
              }


# Update the Styles
for k, v in style_dict.iteritems():
  mpl.rcParams[k] = v

if __name__ == '__main__':
  pass
