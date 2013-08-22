# NZEM:


Author: Nigel Cleland

Version: 0.2.0

Analysis Scripts for the New Zealand Electricity Market.
Mainly a personal package intended to facilitate rapid data analysis where
needed as well as long term analysis.
Broadly this is the stuff which makes my life easier.

Currently in a major transition period:
Broadly, have a bit of extra time at the moment so I can reintroduce nzem to
active development. This will be extensive and the prior submodules will
likely all be replaced with more appropraite modules

In addition it is planned to write tests for these, will need to think about
this more however.

I need to figure out how to use Sphinx to automatically create documentation
As well as automate the package side of things to make this more useful.
I also need to write a bunch more tests...

# Modules under active development

## wits

A module to make it much much easier to work with the files from the WITS site.
The site is pretty archaic and serves files with confusing URLs, strange hyperlinks
and as compressed files.

This module should make it easier to import WITS data from the website.


## plotting

Predefined plots, I dislike the currently implemented matplotlib defaults and
typically find I spend a large proportion of my day reworking these into something
acceptible for publication, or even just general perusal.
Currently haven't really extended this very far at the moment.
Still thinking about what I need to do here.

Most likely I should implement a custom style sheet at the very least.

## offers

A module to make working with offer data, both energy, reserve and demand, much
more comfortable. This may include stacking data, specific analysis into subsets
of the data or other arrangements.

This provides the Offer set of classes which should make working with Offer
data a much more pleasant experience.
Includes automatic mapping, stacking, date time conversions, locationality etc
Can also conduct naive clearing assessments as this came up in another project
of mine.

## Gnash

Interface for interacting with the CDS Gnash executable from linux using Python
This can currently handle simple user defined queries.
Eventually it is intended to build this out to handle more exotic use cases.
E.g. use the Gnasher class to precompile frequently used queries
and expose these to the user in a nicer fashion.
