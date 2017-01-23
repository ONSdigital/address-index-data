#!/usr/bin/env python
"""
ONS Address Index - Data
========================

A simple script containing methods to query or modify the ONS AddressBase data.


Requirements
------------

:requires: numpy
:requires: pandas
:requires: sqlalchemy
:requires: tqdm (https://github.com/tqdm/tqdm)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.9
:date: 23-Jan-2016
"""
import glob
import os
import re
import sqlite3
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

if os.environ.get('LC_CTYPE', '') == 'UTF-8':
    os.environ['LC_CTYPE'] = 'en_US.UTF-8'


def queryDB(sql, connection='postgresql://postgres@localhost/ONSAI'):
    """
    Query PostGre ONS AI database. The default connection is to a local copy.

    :param sql: query to be executed
    :type sql: str
    :param connection: definition of the connection over which to query
    :type connection: str

    :return: results of the query in a pandas dataframe
    :rtype: pandas.DataFrame

    """
    disk_engine = create_engine(connection)
    df = pd.read_sql_query(sql, disk_engine)

    return df


def getPostcode(string):
    """
    Extract a postcode from address information.

    Uses regular expression to extract the postcode:
    http://regexlib.com/REDetails.aspx?regexp_id=260&AspxAutoDetectCookieSupport=1

    :param string: string to be parsed
    :type string: str

    :return: postcode
    :rtype: str
    """
    try:
        tmp = \
        re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]? {1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA',
                   string)[0]
    except ValueError:
        tmp = None

    return tmp


def _getPostcode(row, column='address'):
    """
    A wrapper to getPostcode so that it can be called using dataframe.apply
    """
    return getPostcode(row[column])


def _removePostcode(row, column='address', postcode='postcode'):
    """
    A function to remove postcode from the string in column.

    :param row: pandas dataframe row
    :param column: name of the column from which the postcode gets removed
    :param postcode: name of the column where the postcode is stored

    :return:
    """
    return row[column].replace(row[postcode], '')


def testParsing():
    """

    :return:
    """
    testQuery = 'SELECT address, uprn FROM addresses limit 10'
    df = queryDB(testQuery)
    df['postcode'] = df.apply(_getPostcode, axis=1)
    print(df)


def _simpleTest():
    """

    :return:
    """
    testQuery = 'SELECT COUNT(*) FROM abp_blpu'
    df = queryDB(testQuery)
    print(df)

    testQuery = 'SELECT * FROM abp_delivery_point limit 10'
    df = queryDB(testQuery)
    print(df)

    testQuery = 'SELECT * FROM addresses limit 10'
    df = queryDB(testQuery)
    print(df)


def combineMiniABtestingData():
    """
    Read in a subset of AddressBase Epoch 39 data and combine to a single CSV file.
    A subset of the AddressBase was produced by ONS to provide early access to AB
    without needing to move the full AddressBase out of ONS network.

    :return: None
    """
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/miniAB/'
    files = glob.glob(path + '*.csv')

    for file in files:
        if 'CLASSIFICATION' in file or 'SREET.csv' in file:
            pass

        tmp = pd.read_csv(file, dtype=str)

        if 'BLPU' in file:
            BLPU = tmp[['UPRN', 'POSTCODE_LOCATOR']]

        if 'DELIVERY_POINT' in file:
            DP = tmp[['UPRN', 'ORGANISATION_NAME', 'DEPARTMENT_NAME', 'SUB_BUILDING_NAME',
                      'BUILDING_NAME', 'BUILDING_NUMBER', 'THROUGHFARE', 'DEPENDENT_LOCALITY',
                      'POST_TOWN', 'POSTCODE']]

        if 'LPI' in file:
            LPI = tmp[['UPRN', 'USRN', 'PAO_TEXT', 'PAO_START_NUMBER', 'SAO_TEXT', 'SAO_START_NUMBER', 'LANGUAGE']]

        if 'STREET_DESC' in file:
            ST = tmp[['USRN', 'STREET_DESCRIPTOR', 'TOWN_NAME', 'LANGUAGE', 'LOCALITY']]

        if 'ORGANISATION' in file:
            ORG = tmp[['UPRN', 'ORGANISATION']]

    # join the various dataframes
    data = pd.merge(BLPU, DP, how='left', on='UPRN')
    data = pd.merge(data, LPI, how='left', on='UPRN')
    data = pd.merge(data, ORG, how='left', on=['UPRN'])
    data = pd.merge(data, ST, how='left', on=['USRN', 'LANGUAGE'])

    # drop if all null
    data.dropna(inplace=True, how='all')

    # change uprn to int
    data['UPRN'] = data['UPRN'].astype(int)

    # drop if no UPRN
    data = data[np.isfinite(data['UPRN'].values)]

    # drop some that are not needed
    data.drop(['LANGUAGE', 'USRN'], axis=1, inplace=True)

    print(data.info())
    print(len(data.index), 'addresses')

    # save to a file
    data.to_csv(path + 'ABmini.csv', index=0)


def combineAddressBaseData(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                           filename='AB.csv'):
    """
    Read in all the Address Base Epoch 39 CSV files and combine to a single CSV file.
    Only relevant information is retained to compress the AB for easier handling.

    :param path: location of the AddressBase CSV files
    :type path: str
    :param filename: name of the output file
    :type filename: str

    :return: None
    """
    files = glob.glob(path + 'ABP_E39_*.csv')

    for file in tqdm(files):

        # skip a few addresses not used
        if 'CLASSIFICATION' in file or 'STREET.csv' in file:
            pass

        print('Reading in', file)
        tmp = pd.read_csv(file, dtype=str)

        if 'BLPU' in file:
            BLPU = tmp[['UPRN', 'POSTCODE_LOCATOR']]

        if 'DELIVERY_POINT' in file:
            DP = tmp[['UPRN', 'ORGANISATION_NAME', 'DEPARTMENT_NAME', 'SUB_BUILDING_NAME',
                      'BUILDING_NAME', 'BUILDING_NUMBER', 'THROUGHFARE', 'DEPENDENT_LOCALITY',
                      'POST_TOWN', 'POSTCODE']]

        if 'LPI' in file:
            LPI = tmp[['UPRN', 'USRN', 'LANGUAGE',
                       'PAO_TEXT', 'PAO_START_NUMBER', 'PAO_START_SUFFIX', 'PAO_END_NUMBER', 'PAO_END_SUFFIX',
                       'SAO_TEXT', 'SAO_START_NUMBER', 'SAO_START_SUFFIX']]

        if 'STREET_DESC' in file:
            ST = tmp[['USRN', 'STREET_DESCRIPTOR', 'TOWN_NAME', 'LANGUAGE', 'LOCALITY']]

        if 'ORGANISATION' in file:
            ORG = tmp[['UPRN', 'ORGANISATION']]

    print('joining the individual dataframes...')
    data = pd.merge(BLPU, DP, how='left', on='UPRN')
    data = pd.merge(data, LPI, how='left', on='UPRN')
    data = pd.merge(data, ORG, how='left', on=['UPRN'])
    data = pd.merge(data, ST, how='left', on=['USRN', 'LANGUAGE'])

    # drop if all null
    data.dropna(inplace=True, how='all')

    # change uprn to int
    data['UPRN'] = data['UPRN'].astype(int)

    # drop if no UPRN
    data = data[np.isfinite(data['UPRN'].values)]

    # drop some that are not needed
    data.drop(['LANGUAGE', 'USRN'], axis=1, inplace=True)

    print(data.info())
    print(len(data.index), 'addresses')

    print('storing to a CSV file...')
    data.to_csv(path + filename, index=False)


def processPostcodeFile():
    """
    Modify the scraped postcode file, for example, remove if 'shared' appears
    in any of the postcodes.

    :return: None
    """
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/old/'
    df = pd.read_csv(path + 'postcodefile.csv')

    b = pd.DataFrame(df.PostcodeDistricts.str.split(',').tolist(), index=df.PostTown).stack()
    b = b.reset_index()[[0, 'PostTown']]  # var1 variable is currently labeled 0
    b.columns = ['PostTown', 'PostcodeDistricts']  # renaming var1

    # lower case and stripping
    b['PostTown'] = b['PostTown'].apply(lambda x: x.strip().lower().replace('shared', ''))
    b['PostcodeDistricts'] = b['PostcodeDistricts'].apply(lambda x: x.strip().lower())

    b.to_csv(path + 'postcodefileProcessed.csv', index=False)


def modifyEdgeCasesData():
    """
    Remove postcodes from the ORDER_MATTER Edge case examples.

    The way Neil created these entries there are two ways of matching.
    The original idea was that the town is correct but the postcode is incorrect.
    However, one could take the opposite approach and get matches. Because of this
    it was decided that it is easiest to remove the postcodes to remove the possibility
    of misunderstanding.

    :return: None
    """
    df = pd.read_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K.csv')
    print(df.info())

    # remove postcode
    df['postcode'] = df.apply(_getPostcode, args=('ADDRESS',), axis=1)
    msk = df['MNEMONIC'] == 'ORDER_MATTERS'
    df['ADDRESS'].loc[msk] = df.loc[msk].apply(_removePostcode, args=('ADDRESS', 'postcode'), axis=1)
    df.drop(['postcode', ], axis=1, inplace=True)

    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K_NoPostcode.csv', index=False)

    print(df.info())


def convertCSVtoSQLite(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/', csvFile='AB.csv'):
    """
    Converts a AddressBase CSV file to SQLite3 database.

    :param path: location of the AddressBase file
    :type path: str
    :param csvFile: name of the CSV file containing a subset of AB data
    :type csvFile: str

    :return: None
    """
    df = pd.read_csv(path + csvFile)
    print(df.info())

    connection = path + csvFile.replace('.csv', '.sqlite')
    with sqlite3.connect(connection) as cnx:
        df.to_sql('ab', cnx, index=False, if_exists='replace')


def create_NLP_index(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/', filename='NLPindex.csv'):
    """
    Read in all the Address Base Epoch 39 CSV files and combine to a single NLP index.

    :param path: location of the AddressBase CSV files
    :type path: str
    :param filename: name of the output file
    :type filename: str

    :return: None
    """
    files = glob.glob(path + 'ABP_E39_*.csv')

    columns = {'BLPU': ['UPRN', 'POSTCODE_LOCATOR'],
               'LPI': ['UPRN', 'USRN', 'LANGUAGE', 'PAO_TEXT', 'PAO_START_NUMBER', 'PAO_START_SUFFIX', 'PAO_END_NUMBER',
                       'PAO_END_SUFFIX','SAO_TEXT', 'SAO_START_NUMBER', 'SAO_START_SUFFIX', 'OFFICIAL_FLAG'],
               'STREET_DESC': ['USRN', 'STREET_DESCRIPTOR', 'TOWN_NAME', 'LANGUAGE', 'LOCALITY'],
               'ORGANISATION': ['UPRN', 'ORGANISATION']}

    data_frames = {}
    for file in tqdm(files):
        if 'CLASSIFICATION' in file or 'STREET.csv' in file or 'DELIVERY_POINT' in file:
            continue

        print('Reading in', file)
        for keys in columns:
            if keys in file:
                tmp = pd.read_csv(file, dtype=str, usecols=columns[keys])
                data_frames[keys] = tmp

    # remove non-official addresses from LPI; these can include e.g. ponds, sub stations, cctvs, public phones, etc.
    msk = data_frames['LPI']['OFFICIAL_FLAG'] == 'Y'
    data_frames['LPI'] = data_frames['LPI'].loc[msk]

    print('joining the individual data frames...')
    data = pd.merge(data_frames['BLPU'], data_frames['LPI'], how='right', on='UPRN')
    data = pd.merge(data, data_frames['ORGANISATION'], how='left', on=['UPRN'])
    data = pd.merge(data, data_frames['STREET_DESC'], how='left', on=['USRN', 'LANGUAGE'])

    # drop if all null
    data.dropna(inplace=True, how='all')

    # change uprn to int
    data['UPRN'] = data['UPRN'].astype(int)

    # drop if no UPRN
    data = data[np.isfinite(data['UPRN'].values)]

    # drop some that are not needed
    data.drop(['LANGUAGE', 'USRN'], axis=1, inplace=True)

    print(data.info())
    print(len(data.index), 'addresses')

    print('storing to a CSV file...')
    data.to_csv(path + filename, index=False)


def create_random_sample_of_delivery_point_addresses(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                                                     size=100000):
    """

    :param path:
    :param size:
    :return:
    """
    # read in delivery point table
    delivery_point = pd.read_csv(path + 'ABP_E39_DELIVERY_POINT.csv',  dtype=str)
    delivery_point['UPRN'] = delivery_point['UPRN'].astype(np.int64)
    print(len(delivery_point.index), 'delivery point addresses...')

    # read in the UPRNs from NLP index
    nlp_index = pd.read_csv(path + 'NLPindex.csv', usecols=['UPRN', ])
    nlp_index.drop_duplicates('UPRN', inplace=True)
    print(len(nlp_index.index), 'NLP index entries...')

    # make an inner join between delivery point and NLP
    data = pd.merge(delivery_point, nlp_index, how='inner', on='UPRN', suffixes=('', '_duplicate'))
    print(data.info())
    print(len(data.index), 'addresses...')

    # pick random sample and drop column
    data = data.sample(n=size, random_state=42)

    # write to a file - UPRN and a single string address from the delivery point
    data = data.fillna('')
    data['ADDRESS'] = data["ORGANISATION_NAME"] + ' ' + data["DEPARTMENT_NAME"] + ' ' + data["SUB_BUILDING_NAME"] + ' '\
                      + data["BUILDING_NAME"] + ' ' + data["BUILDING_NUMBER"] + ' ' + data["THROUGHFARE"] + ' ' +\
                      data["POST_TOWN"] + ' ' + data["POSTCODE"]

    data = data[['UPRN', 'ADDRESS']]
    print('Storing single string delivery point addresses to a file...')
    data.to_csv(path + 'delivery_point_addresses.csv', index=False)


if __name__ == "__main__":
    create_NLP_index()
    create_random_sample_of_delivery_point_addresses()