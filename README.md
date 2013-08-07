# NZEM:


Author: Nigel Cleland

Version: 0.1.0

Analysis Scripts for the New Zealand Electricity Market.
Mainly a personal package intended to facilitate rapid data analysis where
needed as well as long term analysis.

Currently in a major transition period:
Broadly, have a bit of extra time at the moment so I can reintroduce nzem to 
active development. This will be extensive and the prior submodules will
likely all be replaced with more appropraite modules

In addition it is planned to write tests for these, will need to think about
this more however.

# Modules under active development

## wits

A module to make it much much easier to work with the files from the WITS site.
The site is pretty archaic and serves files with confusing URLs, strange hyperlinks
and as compressed files.

This module should make it easier to work with

## plotting

Predefined plots, I dislike the currently implemented matplotlib defaults and
typically find I spend a large proportion of my day reworking these into something
acceptible for publication, or even just general perusal.

## offers

A module to make working with offer data, both energy, reserve and demand, much
more comfortable. This may include stacking data, specific analysis into subsets
of the data or other arrangements.

## Gnash

Interface for interacting with the CDS Gnash executable from linux using Python

