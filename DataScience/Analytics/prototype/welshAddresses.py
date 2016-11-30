#!/usr/bin/env python
"""
ONS Address Index - Welsh Addresses Test
========================================

A simple script to attach UPRNs to Welsh test data. The data set contains UPRNs that were
attached with another technique, so the comparison is carried out against those attached
earlier.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python welshAddresses.py


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


Results
-------

With full AB and a reasonable runtime (AddressLinker version 0.1):
    Total Match Fraction 97.9 per cent
    96029 previous UPRNs in the matched data
    95522 addresses have the same UPRN as earlier
    158 addresses have a different UPRN as earlier
    32592 more addresses with UPRN
"""
from Analytics.linking import addressLinking
import pandas as pd


class WelshAddressLinker(addressLinking.AddressLinker):
    """
    Address Linker for Welsh Test data. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the Welsh address test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        # fill NaNs with empty strings so that we can form a single address string
        self.toLinkAddressData.fillna('', inplace=True)
        self.toLinkAddressData['ADDRESS'] = self.toLinkAddressData['Building'] + ' ' + \
                                            self.toLinkAddressData['Street'] + ' ' + \
                                            self.toLinkAddressData['Locality'] + ' ' + \
                                            self.toLinkAddressData['Town'] + ' ' + \
                                            self.toLinkAddressData['County'] + ' ' + \
                                            self.toLinkAddressData['Postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        self.toLinkAddressData.rename(columns={'UPRNs_matched_to_date': 'UPRN_old'}, inplace=True)

        # convert original UPRN to numeric
        self.toLinkAddressData['UPRN_old'] = self.toLinkAddressData['UPRN_old'].convert_objects(convert_numeric=True)


def run_welsh_address_linker():
    """
    A simple wrapper that allows running Welsh Address linker.

    :return: None
    """
    settings = dict(inputFilename='WelshGovernmentData21Nov2016.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='WelshGov')

    linker = WelshAddressLinker(**settings)
    linker.run_all()


if __name__ == "__main__":
    run_welsh_address_linker()
