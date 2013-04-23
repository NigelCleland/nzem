"""
Series of small functions to make the application of helper functions easier
for a range of aggregations
"""

import pandas as pd
import numpy as np

def per10(x):
    return np.percentile(x, q=10)
    
def per25(x):
    return np.percentile(x, q=25)
    
def per50(x):
    return np.percentile(x, q=50)
    
def per75(x):
    return np.percentile(x, q=75)
    
def per90(x):
    return np.percentile(x, q=90)

if __name__ == '__main__':
    pass
