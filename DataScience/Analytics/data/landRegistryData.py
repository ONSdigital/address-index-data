#!/usr/bin/env python
"""
ONS Address Index - Land Registry Data
======================================

A simple script to process land registry sales data.

The original data were downloaded on the 10th of November from:
https://data.gov.uk/dataset/land-registry-monthly-price-paid-data
Because the AddressBased used by the prototype is Epoch 39 (from April) it does not contain all
new builds with new postcodes. This scripts allows to identify those postcodes that do not exist
in the Epoch 39 AddressBase.


Running
-------

The script can be run from command line using CPython::

    python landRegistryData.py


Requirements
------------

:requires: pandas
:requires: numpy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 18-Nov-2016

"""
import pandas as pd
import numpy as np
import os


if os.environ.get('LC_CTYPE', '') == 'UTF-8':
    os.environ['LC_CTYPE'] = 'en_US.UTF-8'


def loadData(filename='pp-monthly-update.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/'):
    """
    Read in the Land Registry testing data.

    The data were downloaded from:
    https://data.gov.uk/dataset/land-registry-monthly-price-paid-data
    The header was grabbed from:
    https://www.gov.uk/guidance/about-the-price-paid-data#explanations-of-column-headers-in-the-ppd

    :param filename: name of the CSV file holding the data
    :type filename: str
    :param path: location of the test data
    :type path: str

    :return: pandas dataframe of the data (no UPRNs)
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(path + filename, low_memory=False, parse_dates=[2, ], infer_datetime_format=True)

    print('Found', len(df.index), 'addresses from the land registry sales data...')

    return df


def loadAddressBaseData(filename='AB.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/'):
    """
    Load a compressed version of the full AddressBase file. The information being used
    has been processed from a AB Epoch 39 files provided by ONS.

    :param filename: name of the file containing modified AddressBase
    :type filename: str
    :param path: location of the AddressBase combined data file
    :type path: str

    :return: pandas dataframe of the requested information
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(path + filename, usecols=['POSTCODE', 'POSTCODE_LOCATOR'])
    print('Found', len(df.index), 'addresses from AddressBase...')

    # combine PAF and NAG information
    msk = df['POSTCODE'].isnull()
    df.loc[msk, 'POSTCODE'] = df.loc[msk, 'POSTCODE_LOCATOR']

    return df


def testIfPostcodeExists(ab, landRegistry):
    """
    A simple function to identify those postcodes that are present in the land registry data but
    missing from AddressBase. Most of these are new buildings. One should consider removing these
    from the testing of prototype matching.

    :param ab: dataframe containing addressbase information
    :type ab: pandas.DataFrame
    :param landRegistry: dataframe containing land registry data
    :type landRegistry: pandas.DataFrame

    :return: None
    """
    # find unique postcodes from AddressBase
    ABpostcodes = np.unique(ab['POSTCODE'].values)

    # those land registry postcodes that are not present in AddressBase are newbuilds
    msk = landRegistry['Postcode'].isin(ABpostcodes)

    # get those addresses that have a postcode in AB and identify missing postcodes
    lr = landRegistry.loc[~msk]
    missingPostcodes = np.unique(lr.loc[lr['Postcode'].notnull(), 'Postcode'].values)

    print('Missing Postcodes:')
    print(missingPostcodes)
    print('In total', len(missingPostcodes), 'postcodes in sales data without AB counterpart')
    print('In total', len(lr.index), 'addresses without counterparts')

    # find those with postcode counterparts and save to a file
    msk = ~landRegistry.Postcode.isin(missingPostcodes)
    lr = landRegistry.ix[msk]
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/'
    print('After removing postcodes without counterpart', len(lr.index), 'address remain...')
    lr.to_csv(path + 'pp-monthly-update-Edited.csv', index=False)

    # record also those without postcode counterpart
    lr = landRegistry.ix[~msk]
    print(len(lr.index), 'addresses without postcodes...')
    lr.to_csv(path + 'pp-monthly-update-no-postcode.csv', index=False)


if __name__ == "__main__":
    ab = loadAddressBaseData()
    lr = loadData()
    testIfPostcodeExists(ab, lr)
