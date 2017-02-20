#!/usr/bin/env python
"""
ONS Address Index - Welsh Addresses Test, Third Dataset
=======================================================

A simple script to attach UPRNs to the third Welsh test dataset. The data set contains UPRNs that were
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

    python welshAddressesSet3.py


Requirements
------------

:requires: pandas (0.19.1)
:requires: addressLinking (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 20-Feb-2017
"""
import pandas as pd
from Analytics.linking import addressLinking


class WelshAddressLinker(addressLinking.AddressLinker):
    """
    Address Linker for the third Welsh Test dataset. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the Welsh address test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        columns = list(self.toLinkAddressData.columns.values)
        columns.remove('URN')
        columns.remove('UPRN')

        # fill NaNs with empty strings so that we can form a single address string
        self.toLinkAddressData[columns] = self.toLinkAddressData[columns].fillna('')

        self.toLinkAddressData['ADDRESS'] = self.toLinkAddressData['Service_Name'] + ' ' + \
                                            self.toLinkAddressData['Line1'] + ' ' + \
                                            self.toLinkAddressData['Line2'] + ' ' + \
                                            self.toLinkAddressData['Line3'] + ' ' + \
                                            self.toLinkAddressData['Postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        self.toLinkAddressData.rename(columns={'UPRN': 'UPRN_old', 'URN': 'ID'}, inplace=True)
        self.toLinkAddressData.drop(columns, axis=1, inplace=True)


def run_welsh_address_linker(**kwargs):
    """
    A simple wrapper that allows running Welsh Address linker.

    :return: None
    """
    settings = dict(inputFilename='Welsh_Gov_3rd_set.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='WelshGovSet3')
    settings.update(kwargs)

    linker = WelshAddressLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_welsh_address_linker()
