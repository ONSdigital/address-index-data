#!/usr/bin/env python
"""
ONS Address Index - Patient Records Test
========================================

A simple script to attach UPRNs to Patient Records test data. The data set contains UPRNs that were
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

    python patientRecordAddresses.py


.. Note:: This dataset cannot leave ONS network and thus the script can only be run on an ONS network node.


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
:date: 2-Dec-2016
"""
from Analytics.linking import addressLinking
import pandas as pd


class PatientRecordsAddressLinker(addressLinking.AddressLinker):
    """
    Address Linker for Patient Records test data. Inherits the AddressLinker and overwrites the load_data method.
    """

    def load_data(self):
        """
        Read in the Patient Record address test data. Overwrites the method in the AddressLinker.
        """
        self.toLinkAddressData = pd.read_excel(self.settings['inputPath'] + self.settings['inputFilename'])

        self.toLinkAddressData.drop(['TRUTH_CONFIDENCE', 'primary_uprn', 'alt_UPRN', 'alt_primary_uprn',
                                     'layers', 'this_layer', 'Ambiguous_match', 'low_ambiguity_USRN'],
                                    axis=1, inplace=True)

        self.toLinkAddressData.rename(columns={'id': 'ID', 'uprn': 'UPRN_old'}, inplace=True)


def run_patient_record_address_linker(**kwargs):
    """
    A simple wrapper that allows running the patient record address linker.

    :return: None
    """
    settings = dict(inputFilename='RW100K.xlsx',
                    inputPath='/opt/scratch/AddressIndex/TestData/',
                    outpath='/opt/scratch/AddressIndex/Results/',
                    outname='PatientRecord',
                    ABpath='/opt/scratch/AddressIndex/AddressBase/')
    settings.update(kwargs)

    linker = PatientRecordsAddressLinker(**settings)
    linker.run_all()


if __name__ == "__main__":
    run_patient_record_address_linker()
