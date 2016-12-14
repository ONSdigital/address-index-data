#!/usr/bin/env python
"""
ONS Address Index - Life Events Testing
=======================================

A simple script to test the life events data. In general there are no UPRNs in this data,
hence the most one can do is the get the linking rate and clerically review whether the
found matches are correct or not.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python lifeEventsAddresses.py


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
:date: 14-Dec-2016
"""
from Analytics.linking import addressLinking
import pandas as pd
import numpy as np


class LifeEventsLinker(addressLinking.AddressLinker):
    """
    Address Linker for Life Events test data. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        # change column names
        self.toLinkAddressData.rename(columns={'AddressInput': 'ADDRESS', 'UPRN': 'UPRN_old'}, inplace=True)

    def load_addressbase(self):
        """
        Overwrite the standard address base loading method to optimise memory consumption.
        """
        self.log.info('Reading in Modified Address Base Data...')

        self.addressBase = pd.read_csv(self.settings['ABpath'] + 'AB_modified.csv',
                                       dtype={'UPRN': np.int64, 'ORGANISATION_NAME': str,
                                              'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                                              'BUILDING_NUMBER': str, 'THROUGHFARE': str,
                                              'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                                              'PAO_START_NUMBER': str, 'PAO_START_SUFFIX': str,
                                              'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                                              'STREET_DESCRIPTOR': str, 'TOWN_NAME': str, 'LOCALITY': str,
                                              'postcode_in': str, 'postcode_out': str})
        self.log.info('Found {} addresses from AddressBase...'.format(len(self.addressBase.index)))

        self.log.info('Using {} addresses from AddressBase for matching...'.format(len(self.addressBase.index)))

        # set index name - needed later for merging / duplicate removal
        self.addressBase.index.name = 'AddressBase_Index'


def run_life_events_linker(**kwargs):
    """
    A simple wrapper that allows running Life Events linker.

    :return: None
    """
    settings = dict(inputFilename='LifeEventsConsolidated.csv',
                    inputPath='/opt/scratch/AddressIndex/TestData/',
                    outpath='/opt/scratch/AddressIndex/Results/',
                    outname='LifeEvents',
                    ABpath='/opt/scratch/AddressIndex/AddressBase/')

    settings.update(kwargs)

    linker = LifeEventsLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_life_events_linker()
