"""
Determine whether constraints are active for a particular trading period
"""

def HVDC_Constraint(series, energy_price_send="", energy_price_receive="",
                    res_price="", abs_tol=None, rel_tol=None, min_split=10):
    """ 
    Determine whether an HVDC constraint is active within the system
    
    Parameters
    ----------
    series : The Pandas DF series (i.e. a single entry)
    energy_price : The energy price column name
    res_price : The reserve price column name
    inverse : Whether the inverse should be applied (e.g. multiply by -1)
    abs_tol : What absolute tolerance should be applied to the dataset
    rel_tol : What relative tolerance should be applied
    minimium_split : The minimum split required to attribute towards reserve
                     binding upon the system.
                     
    Returns
    -------
    constraint : True if constraint is binding, false otherwise
    """
    
    epr_send = series[energy_price_send]
    epr_rece = series[energy_price_receive]
    rpr = series[res_price]
    
    split = epr_rece - epr_send
    if split <= min_split:
        return False
    else:
        if abs_tol:
            return True if abs(split - rpr) <= abs_tol else False
        elif rel_tol:
            return True if abs(split - rpr) <= rel_tol * epr_send else False
        else:
            raise ValueError("Either abs_tol or rel_tol must be specified")

    
