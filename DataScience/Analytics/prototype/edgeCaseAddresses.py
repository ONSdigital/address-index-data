#!/usr/bin/env python
"""
ONS Address Index - Edge Case Testing
=====================================

A simple script to test parsing and matching of edge cases - 5k dataset of different types of addresses.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python edgeCaseAddresses.py


Requirements
------------

:requires: pandas
:requires: addressLinking (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 29-Nov-2016


Results
-------

With full AB and reasonable runtime:
    Matched 1934 entries
    Total Match Fraction 96.7
    Correctly Matched 1921
    Correctly Matched Fraction 96.0
    False Positives 13
    False Positive Rate 0.7
    Correctly Matched 994 DEAD_SIMPLE
    Match Fraction 99.4
    False Positives 0
    False Positive Rate 0.0
    Correctly Matched 927 DEAD_SIMPLE_NO_PC
    Match Fraction 92.7
    False Positives 13
    False Positive Rate 1.3
"""
from Analytics.linking import addressLinking
import pandas as pd


class EdgeCaseLinker(addressLinking.AddressLinker):
    """
    Address Linker for Edge Cases test data. Inherits the AddressLinker and overwrites the load_data method.
    """
    def load_data(self):
        """
        Read in the test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        # change column names
        self.toLinkAddressData.rename(columns={'UPRN': 'UPRN_old', 'MNEMONIC': 'Category'}, inplace=True)


if __name__ == "__main__":
    settings = dict(inputFilename='EDGE_CASES_EC5K_NoPostcode.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='EdgeCases',
                    outpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/')

    linker = EdgeCaseLinker(**settings)
    linker.run_all()
