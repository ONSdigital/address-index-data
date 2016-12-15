#!/usr/bin/env python
"""
ONS Address Index - Companies House Testing
===========================================

A simple script to test the Companies House data received from the Business Index.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python companiesHouse.py


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


class CompaniesHouseLinker(addressLinking.AddressLinker):
    """
    Address Linker for Companies House test data. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        # change column names
        self.toLinkAddressData.rename(columns={'address': 'ADDRESS', 'id': 'ID'}, inplace=True)


def run_companies_house_linker(**kwargs):
    """
    A simple wrapper that allows running Companies House linker.

    :return: None
    """
    settings = dict(inputFilename='CompaniesHouse.csv', outname='CompaniesHouse')

    settings.update(kwargs)

    linker = CompaniesHouseLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_companies_house_linker()
