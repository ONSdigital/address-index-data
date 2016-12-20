#!/usr/bin/env python
"""
ONS Address Index - Welsh Addresses Test, Second Dataset
========================================================

A simple script to attach UPRNs to the second Welsh test dataset. The data set contains UPRNs that were
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

    python welshAddressesSet2.py


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
:date: 20-Dec-2016
"""
from Analytics.linking import addressLinking
import pandas as pd


class WelshAddressLinker(addressLinking.AddressLinker):
    """
    Address Linker for the second Welsh Test dataset. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the Welsh address test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_excel(self.settings['inputPath'] + self.settings['inputFilename'],
                                               encoding='iso-8859-1')
        print(self.toLinkAddressData.info())

        # fill NaNs with empty strings so that we can form a single address string
        self.toLinkAddressData[self.toLinkAddressData.columns.list.remove('uprn')].fillna('', inplace=True)
        self.toLinkAddressData['ADDRESS'] = self.toLinkAddressData['name'] + ' ' + \
                                            self.toLinkAddressData['addressLineOne'] + ' ' + \
                                            self.toLinkAddressData['addressLineTwo'] + ' ' + \
                                            self.toLinkAddressData['addressLineThree'] + ' ' + \
                                            self.toLinkAddressData['addressLineFour'] + ' ' + \
                                            self.toLinkAddressData['postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        self.toLinkAddressData.rename(columns={'uprn': 'UPRN_old', 'schoolCode': 'ID'}, inplace=True)
        self.toLinkAddressData.drop(['addressLineOne', 'addressLineTwo', 'addressLineThree',
                                     'addressLineFour', 'postcode'], axis=1, inplace=True)



def run_welsh_address_linker(**kwargs):
    """
    A simple wrapper that allows running Welsh Address linker.

    :return: None
    """
    settings = dict(inputFilename='Welsh_Gov_2nd_set.xlsx',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='WelshGovSet2',
                    store=False)
    settings.update(kwargs)

    linker = WelshAddressLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_welsh_address_linker()
