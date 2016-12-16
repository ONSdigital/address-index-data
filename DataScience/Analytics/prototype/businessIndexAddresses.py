#!/usr/bin/env python
"""
ONS Address Index - Business Index Testing
==========================================

A simple script to test the Companies House and Charities Commission data received from the Business Index.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python businessIndexAddresses.py


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
:date: 15-Dec-2016
"""
from Analytics.linking import addressLinking
import pandas as pd


class BusinessIndexLinker(addressLinking.AddressLinker):
    """
    Address Linker for Business Index test data. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        self.toLinkAddressData.rename(columns={'address': 'ADDRESS', 'id': 'ID'}, inplace=True)


def run_business_index_linker(**kwargs):
    """
    A simple wrapper that allows running Business Index linker.

    :return: None
    """
    settings = dict(inputFilename='BusinessIndex.csv', outname='BusinessIndex')

    settings.update(kwargs)

    linker = BusinessIndexLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_business_index_linker()
