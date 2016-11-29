#!/usr/bin/env python
"""
ONS Address Index - Land Registry Testing
=========================================

A simple script to attach UPRNs to land registry data. The used data set contains about 102k
addresses, not all residential, for example, there are parking spaces, garages, and plots in
the sales data used. The data were downloaded on the 10th of November from:
https://data.gov.uk/dataset/land-registry-monthly-price-paid-data

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

The script can be run from command line using CPython::

    python landRegistryAddresses.py


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

With full AB and reasonable runtime (i.e. using blocking):
    Total Match Fraction 99.8 per cent
"""
from Analytics.linking import addressLinking
import pandas as pd


class LandRegistryLinker(addressLinking.AddressLinker):
    """
    Address Linker for Land Registry test data. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the Land Registry testing data. Note that the data should be preprocessed with a script
        found in the DataScience/Analytics/data/ folder.

        The data were originally downloaded from:
        https://data.gov.uk/dataset/land-registry-monthly-price-paid-data
        The header was grabbed from:
        https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False, parse_dates=[2, ], infer_datetime_format=True)

        # drop unnecessary information
        self.toLinkAddressData.drop(['Price', 'TransferDate', 'Type', 'New', 'Duration',
                                     'PPD', 'District', 'RecordStatus'],
                                    axis=1, inplace=True)

        # fill NaNs with empty strings so that we can form a single address string
        self.toLinkAddressData.fillna('', inplace=True)
        self.toLinkAddressData['ADDRESS'] = self.toLinkAddressData['SAO'] + ' ' + \
                                            self.toLinkAddressData['PAO'] + ' ' + \
                                            self.toLinkAddressData['Street'] + ' ' + \
                                            self.toLinkAddressData['Locality'] + ' ' + \
                                            self.toLinkAddressData['Town'] + ' ' + \
                                            self.toLinkAddressData['Postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        self.toLinkAddressData.rename(columns={'Postcode': 'postcode_orig',
                                               'Locality': 'locality_orig',
                                               'TransactionID': 'ID'}, inplace=True)


if __name__ == "__main__":
    settings = dict(inputFilename='pp-monthly-update-Edited.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='LandRegistry',
                    outpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/')

    linker = LandRegistryLinker(**settings)
    linker.run_all()
