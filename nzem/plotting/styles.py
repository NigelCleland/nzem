""" Plotting styles for the NZEM module.

Provides a series of colour schemes for consistent plot styling

Currently Implemented:
----------------------
grey_scale: Colour Scheme which uses line types, markets and opacity to
            distinguish between different variables. Intended for black
            and white printing, or publications
colour: Colour scheme which utilises different colours to distinguish
        between plots. Intended for interactive viewing, electronic
        reports or web based requirements.

gen_styles: Generic Styles which are utilised for both kinds of plots

"""

import matplotlib as mpl


gs_line_dict = {'haywards_price': {'c': 'black', 'alpha': 0.7, 'linestyle': '-'},
             'benmore_price': {'c': 'black', 'alpha': 0.3, 'linestyle': '-'},
             'fir_price': {'c': 'black', 'alpha': 0.7, 'linestyle': '--'},
             'sir_price': {'c': 'black', 'alpha': 0.3, 'linestyle': '--'},
             'ni_reserve_price': {'c': 'black', 'alpha': 0.7,
                                  'linestyle': '.-'},
             'si_reserve_price': {'c': 'black', 'alpha': 0.3,
                                  'linestyle': '.-'}
             }

gs_scatter_dict = {

                }

co_line_dict = {}

co_scatter_dict = {}

# Aggregate the style dictionaries together.
colour_schemes = {'greyscale_line': gs_line_dict,
                  'greyscale_scatter': gs_scatter_dict,
                  'colour_line': co_line_dict,
                  'colour_scatter': co_scatter_dict
                  }


style_dict = {'figsize': (16,9),
              'dpi': 100}


# Update the Styles
mpl.rcparam(**style_dict)

if __name__ == '__main__':
