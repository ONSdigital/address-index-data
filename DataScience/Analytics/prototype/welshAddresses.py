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

With full AB and a reasonable runtime (aggressive blocking):
    Total Match Fraction 99.9 per cent

    96029 previous UPRNs in the matched data
    95666 addresses have the same UPRN as earlier
    363 addresses have a different UPRN as earlier
    34955 more addresses with UPRN
"""
from Analytics.linking import addressLinking
import pandas as pd


class WelshAddressLinker(addressLinking.AddressLinker):

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
        self.toLinkAddressData.rename(columns={'UPRNs_matched_to_date': 'UPRN_prev'}, inplace=True)

        # convert original UPRN to numeric
        self.toLinkAddressData['UPRN_prev'] = self.toLinkAddressData['UPRN_prev'].convert_objects(convert_numeric=True)

        if self.settings['verbose']:
            self.log.info(self.toLinkAddressData.info())

        self.log.info('Found {} addresses...'.format(len(self.toLinkAddressData.index)))
        self.nExistingUPRN = len(self.toLinkAddressData.loc[self.toLinkAddressData['UPRN_prev'].notnull()].index)
        self.log.info('{} with UPRN already attached...'.format(self.nExistingUPRN))

        # set index name - needed later for merging / duplicate removal
        self.toLinkAddressData.index.name = 'TestData_Index'

    def check_performance(self):
        """
        Check performance - calculate the match rate using the existing UPRNs. Write the results to a file.
        """
        prefix = self.settings['outname']
        path = self.settings['outpath']

        # count the number of matches and number of edge cases
        nmatched = len(self.matched.index)
        total = len(self.toLinkAddressData.index)

        # how many were matched
        self.log.info('Matched {} entries'.format(nmatched))
        self.log.info('Total Match Fraction {} per cent'.format(round(nmatched / total * 100., 1)))

        # save matched
        self.matched.to_csv(path + prefix + '_matched.csv', index=False)

        # find those without match
        IDs = self.matched['ID'].values
        missing_msk = ~self.toLinkAddressData['ID'].isin(IDs)
        missing = self.toLinkAddressData.loc[missing_msk]
        missing.to_csv(path + prefix + '_matched_missing.csv', index=False)
        self.log.info('{} addresses were not linked...'.format(len(missing.index)))

        nOldUPRNs = len(self.matched.loc[self.matched['UPRN_prev'].notnull()].index)
        self.log.info('{} previous UPRNs in the matched data...'.format(nOldUPRNs))

        # find those with UPRN attached earlier and check which are the same
        msk = self.matched['UPRN_prev'] == self.matched['UPRN']
        matches = self.matched.loc[msk]
        matches.to_csv(path + prefix + '_sameUPRN.csv', index=False)
        self.log.info('{} addresses have the same UPRN as earlier...'.format(len(matches.index)))

        # find those that has a previous UPRN but does not mach a new one (filter out nulls)
        msk = self.matched['UPRN_prev'].notnull()
        notnulls = self.matched.loc[msk]
        nonmatches = notnulls.loc[notnulls['UPRN_prev'] != notnulls['UPRN']]
        nonmatches.to_csv(path + prefix + '_differentUPRN.csv', index=False)
        self.log.info('{} addresses have a different UPRN as earlier...'.format(len(nonmatches.index)))

        # find all newly linked
        newUPRNs = self.matched.loc[~msk]
        newUPRNs.to_csv(path + prefix + '_newUPRN.csv', index=False)
        self.log.info('{} more addresses with UPRN...'.format(len(newUPRNs.index)))


if __name__ == "__main__":
    settings = dict(inputFilename='WelshGovernmentData21Nov2016.csv',
                    inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                    outname='WelshGov',
                    outpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/')

    linker = WelshAddressLinker(**settings)
    linker.run_all()

