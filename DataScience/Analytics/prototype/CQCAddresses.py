#!/usr/bin/env python
"""
ONS Address Index - CQC Dataset Test
====================================

A simple script to attach UPRNs to a CQC dataset that was provided by GeoPlace on 27th of January 2017.
This dataset has UPRNs attached by GeoPlace.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python CQCAddresses.py


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
:date: 30-Jan-2017
"""
from Analytics.linking import addressLinking
import pandas as pd


class CQCAddressLinker(addressLinking.AddressLinker):
    """
    Address Linker for the CQC test dataset. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the CQC address test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             encoding='utf-8')

        columns = list(self.toLinkAddressData.columns.values)
        columns.remove('uprn')
        columns.remove('cqc_loc_id')

        # fill NaNs with empty strings so that we can form a single address string
        self.toLinkAddressData[columns] = self.toLinkAddressData[columns].fillna('')

        self.toLinkAddressData['ADDRESS'] = self.toLinkAddressData['name'] + ' ' + \
                                            self.toLinkAddressData['address'] + ' ' + \
                                            self.toLinkAddressData['postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        self.toLinkAddressData.rename(columns={'uprn': 'UPRN_old', 'cqc_loc_id': 'ID'}, inplace=True)
        self.toLinkAddressData.drop(columns, axis=1, inplace=True)


def run_CQC_address_linker(**kwargs):
    """
    A simple wrapper that allows running CQC Address linker.

    :return: None
    """
    settings = dict(inputFilename='CQC_2016_12_14.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='CQC')
    settings.update(kwargs)

    linker = CQCAddressLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_CQC_address_linker()
