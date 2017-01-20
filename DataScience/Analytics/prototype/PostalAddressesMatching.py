#!/usr/bin/env python
"""
ONS Address Index -
===========================================

A simple script to test the  data linking.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python PostalAddressesMatching.py


Requirements
------------

:requires: pandas (0.19.1)
:requires: addressLinkingNLPindex (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 19-Jan-2017
"""
from Analytics.linking import addressLinkingNLPindex
import pandas as pd


class PostalAddressLinker(addressLinkingNLPindex.AddressLinkerNLPindex):
    """
    Address Linker for Postal Addresses test data.
    Inherits the AddressLinkerNLPindex and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        self.toLinkAddressData['ID'] = self.toLinkAddressData['UPRN'].copy()


def run_postal_addresses_linker(**kwargs):
    """
    A simple wrapper that allows running Postal Addresses linker.

    :return: None
    """
    settings = dict(inputFilename='delivery_point_addresses.csv', outname='DeliveryPointAddresses')

    settings.update(kwargs)

    linker = PostalAddressLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_postal_addresses_linker()
