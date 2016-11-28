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

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: pandas
:requires: numpy
:requires: tqdm (https://github.com/tqdm/tqdm)
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.4
:date: 23-Nov-2016


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

    def load_data(self,
                 filename='WelshGovernmentData21Nov2016.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                 verbose=False, test=False):
        """
        Read in the Welsh address test data.

        :param filename: name of the CSV file holding the data
        :type filename: str
        :param path: location of the test data
        :type path: str
        :param verbose: whether or not output information
        :type verbose: bool
        :param test: whether or not to use test data
        :type test: bool

        :return: pandas dataframe of the data
        :rtype: pandas.DataFrame
        """
        if test:
            print('\nReading in test Welsh Government data...')
            filename = 'WelshTest.csv'
        else:
            print('\nReading in Welsh Government data...')

        df = pd.read_csv(path + filename, low_memory=False)

        # fill NaNs with empty strings so that we can form a single address string
        df.fillna('', inplace=True)
        df['ADDRESS'] = df['Building'] + ' ' + df['Street'] + ' ' + df['Locality'] + ' ' + df['Town'] +\
                        ' ' + df['County'] + ' ' + df['Postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        df.rename(columns={'UPRNs_matched_to_date': 'UPRN_prev'}, inplace=True)

        # convert original UPRN to numeric
        df['UPRN_prev'] = df['UPRN_prev'].convert_objects(convert_numeric=True)

        if verbose:
            print(df.info())

        print('Found', len(df.index), 'addresses...')
        print(len(df.loc[df['UPRN_prev'].notnull()].index), 'with UPRN already attached...')

        return df

    def check_performance(self, df, linkedData,
                         prefix='WelshGov', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/'):
        """
        Check performance - calculate the match rate.

        :param df: data frame with linked addresses and similarity metrics
        :type df: pandas.DataFrame
        :param linkedData: input data, used to identify e.g. those addresses not linked
        :type linkedData: pandas.DataFrame
        :param prefix: prefix name for the output files
        :type prefix: str
        :param path: location where to store the output files
        :type path: str

        :return: None
        """
        # count the number of matches and number of edge cases
        nmatched = len(df.index)
        total = len(linkedData.index)

        # how many were matched
        print('Matched', nmatched, 'entries')
        print('Total Match Fraction', round(nmatched / total * 100., 1), 'per cent')

        # save matched
        df.to_csv(path + prefix + '_matched.csv', index=False)

        # find those without match
        IDs = df['ID'].values
        missing_msk = ~linkedData['ID'].isin(IDs)
        missing = linkedData.loc[missing_msk]
        missing.to_csv(path + prefix + '_matched_missing.csv', index=False)
        print(len(missing.index), 'addresses were not linked...\n')

        nOldUPRNs = len(df.loc[df['UPRN_prev'].notnull()].index)
        print(nOldUPRNs, 'previous UPRNs in the matched data...')

        # find those with UPRN attached earlier and check which are the same
        msk = df['UPRN_prev'] == df['UPRN']
        matches = df.loc[msk]
        matches.to_csv(path + prefix + '_sameUPRN.csv', index=False)
        print(len(matches.index), 'addresses have the same UPRN as earlier...')

        # find those that has a previous UPRN but does not mach a new one (filter out nulls)
        msk = df['UPRN_prev'].notnull()
        notnulls = df.loc[msk]
        nonmatches = notnulls.loc[notnulls['UPRN_prev'] != notnulls['UPRN']]
        nonmatches.to_csv(path + prefix + '_differentUPRN.csv', index=False)
        print(len(nonmatches.index), 'addresses have a different UPRN as earlier...')

        # find all newly linked
        newUPRNs = df.loc[~msk]
        newUPRNs.to_csv(path + prefix + '_newUPRN.csv', index=False)
        print(len(newUPRNs.index), 'more addresses with UPRN...')

if __name__ == "__main__":
    runAll()
