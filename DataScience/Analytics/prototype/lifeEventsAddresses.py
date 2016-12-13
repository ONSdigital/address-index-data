#!/usr/bin/env python
"""
ONS Address Index - Life Events Testing
=======================================

A simple script to test

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

:version: 0.1
:date: 13-Dec-2016
"""
from Analytics.linking import addressLinking
import pandas as pd


class LifeEventsLinker(addressLinking.AddressLinker):
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
        self.toLinkAddressData.rename(columns={'AddressInput': 'ADDRESS', 'UPRN': 'UPRN_old'}, inplace=True)


def run_life_events_linker(**kwargs):
    """
    A simple wrapper that allows running Edge Case linker.

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


if __name__ == "__main__":
    run_life_events_linker()
