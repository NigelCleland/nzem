Gnasher
=======

The Gnasher module is intended to make interfacing with the Gnash CDS data a bit easier.
Currently simple gnash queries can be run against the Gnash Database with the output
returns as Pandas DataFrames with dates and times parsed and formatted a bit nicer.

I'll hopefully extend this to have the module construct its own queries.
This can hopefully make it a bit easier to interact instead of having to hunt through
the Gnash output to figure out what kind of output is necessary.

As a broad first step the output from NAMES should be parsed and sorted.
This is still to do.

Usage
-----

1. Copy the Gnash.Awake.txt file to the directory containing Gnash.
2. Change the directory in the script to where CDS has been extracted to.
3. Run the file

```
%run gnasher.py

G = Gnasher()

# Note, you must leave off the to csvfilename which is usually expected.
G.query_energy("dump ty for 2006")

# Access the query, should return a DataFrame
# Only stores the last class, you should save this to another variable
# if many variables are being run with the same Gnasher object.
G.query 

# Plot it.
G.query["TY"].plot() 
```