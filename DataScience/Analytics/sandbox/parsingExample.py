"""
ONS Address Index - Parsing Example
===================================

A simple demonstration how a probabilistic model can be used for parsing.
Uses an existing solution for the demonstration. For real usage, one should
train a model using for example AddressBase data for the best performance.
One should also consider using different labels (tokens).


Requirements
------------

:requires: libpostal (https://github.com/openvenues/libpostal)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 29-Sep-2016
"""
from postal.parser import parse_address


if __name__ == "__main__":
    print('Example, parsing 6 PROSPECT GARDENS EXETER EX4 6BA:')
    print(parse_address('6 PROSPECT GARDENS EXETER EX4 6BA'))
    var = input('\nInput a string:')
    print(parse_address(var))
