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

:requires: pandas (0.19.1)
:requires: addressLinking (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 30-Nov-2016
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


def run_edge_case_linker(**kwargs):
    """
    A simple wrapper that allows running Edge Case linker.

    :return: None
    """
    settings = dict(inputFilename='EDGE_CASES_EC5K_NoPostcode.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='EdgeCases')
    settings.update(kwargs)

    linker = EdgeCaseLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_edge_case_linker()
