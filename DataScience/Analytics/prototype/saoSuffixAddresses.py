#!/usr/bin/env python
"""
ONS Address Index - Secondary Address Object End Suffix Test Data
=================================================================

A simple script to attach UPRNs to a dataset with SAO end suffixes.

As the dataset is synthetic it contains AddressBase UPRNs enabling automatic
performance computations.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python saoSuffixAddresses.py


Requirements
------------

:requires: numpy (tested with 1.12.0)
:requires: pandas (tested with 0.19.2)
:requires: addressLinking (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 1-Mar-2017
"""
import numpy as np
import pandas as pd
from Analytics.linking import addressLinking


class SAOsuffixLinker(addressLinking.AddressLinker):
    """
    Address Linker for the SAO Suffix dataset. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the SAO Suffix address test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_excel(self.settings['inputPath'] + self.settings['inputFilename'])
        self.toLinkAddressData['ID'] = np.arange(len(self.toLinkAddressData['UPRN'].index))

        self.toLinkAddressData.rename(columns={'UPRN': 'UPRN_old'}, inplace=True)


def run_sao_suffix_linker(**kwargs):
    """
    A simple wrapper that allows running SAO Suffix Address linker.

    :return: None
    """
    settings = dict(inputFilename='SAO_END_SUFFIX.xlsx',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='SAOsuffix')
    settings.update(kwargs)

    linker = SAOsuffixLinker(**settings)
    linker.run_all()
    del linker


if __name__ == "__main__":
    run_sao_suffix_linker()
