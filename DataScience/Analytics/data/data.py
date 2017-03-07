#!/usr/bin/env python
"""
ONS Address Index - Data
========================

A simple script containing methods to query or modify the ONS AddressBase data.

Uses Dask for larger-than-memory computations. Can also use distributed to spread
the computations over a cluster if needed.


Requirements
------------

:requires: dask (tested with 0.14.0)
:requires: distributed (tested with 1.16.0)
:requires: numpy (tested with 1.12.0)
:requires: pandas (tested with 0.19.2)
:requires: sqlalchemy
:requires: tqdm (https://github.com/tqdm/tqdm)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 1.0
:date: 2-Mar-2016
"""
import glob
import os
import re
import sqlite3

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
from distributed import Client, LocalCluster
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
            re.findall(
                r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]? {1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA',
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


def combine_address_base_data(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                              filename='AB.csv', distributed=False):
    """
    Read in all the Address Base Epoch 39 CSV files and combine to a single CSV file.
    Only relevant information is retained to compress the AB for easier handling.

    .. Note:: Uses Dask so that the datasets do not need to fit in memory. This is not very efficient
              as the join is using a column not index. However, as UPRN is not unique using it as index
              has a penalty too.

    :param path: location of the AddressBase CSV files
    :type path: str
    :param filename: name of the output file
    :type filename: str
    :param distributed:
    :type distributed: bool

    :return: None
    """
    if distributed:
        cluster = LocalCluster(n_workers=4, threads_per_worker=1)
        client = Client(cluster)
        print(client)

    all_files = glob.glob(path + 'ABP_E39_*.csv')
    files = [file for file in all_files if ('STREET.csv' not in file)]

    data_container = dict()
    for file in files:
        if 'BLPU' in file:
            columns = ['UPRN', 'POSTCODE_LOCATOR']
            id = 'BLPU'

        if 'DELIVERY_POINT' in file:
            columns = ['UPRN', 'ORGANISATION_NAME', 'DEPARTMENT_NAME', 'SUB_BUILDING_NAME',
                       'BUILDING_NAME', 'BUILDING_NUMBER', 'THROUGHFARE', 'DEPENDENT_LOCALITY',
                       'POST_TOWN', 'POSTCODE']
            id = 'DP'

        if 'LPI' in file:
            columns = ['UPRN', 'USRN', 'LANGUAGE', 'PAO_TEXT', 'PAO_START_NUMBER', 'PAO_START_SUFFIX',
                       'PAO_END_NUMBER', 'PAO_END_SUFFIX', 'SAO_TEXT', 'SAO_START_NUMBER', 'SAO_START_SUFFIX',
                       'SAO_END_NUMBER', 'SAO_END_SUFFIX']
            id = 'LPI'

        if 'STREET_DESC' in file:
            columns = ['USRN', 'STREET_DESCRIPTOR', 'TOWN_NAME', 'LANGUAGE', 'LOCALITY']
            id = 'ST'

        if 'ORGANISATION' in file:
            columns = ['UPRN', 'ORGANISATION']
            id = 'ORG'

        print('Reading in', file)
        data_container[id] = dd.read_csv(file, dtype=str, usecols=columns)

    print('joining the individual data frames to form a single hybrid index...')
    data = dd.merge(data_container['BLPU'], data_container['DP'], how='left', on='UPRN')
    data = dd.merge(data, data_container['LPI'], how='left', on='UPRN')
    data = dd.merge(data, data_container['ORG'], how='left', on=['UPRN'])
    data = dd.merge(data, data_container['ST'], how='left', on=['USRN', 'LANGUAGE'])

    if distributed:
        data = dd.compute(data)[0]
    else:
        with ProgressBar():
            data = dd.compute(data)[0]

    print('change the uprn type to int...')
    data['UPRN'] = data['UPRN'].astype(int)

    print('drop all entries with no UPRN...')
    data = data[np.isfinite(data['UPRN'].values)]

    print('drop unnecessary columns...')
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
                       'PAO_END_SUFFIX', 'SAO_TEXT', 'SAO_START_NUMBER', 'SAO_START_SUFFIX', 'OFFICIAL_FLAG'],
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

    print('joining the individual data frames...')
    data = pd.merge(data_frames['BLPU'], data_frames['LPI'], how='left', on='UPRN')
    data = pd.merge(data, data_frames['ORGANISATION'], how='left', on=['UPRN'])
    data = pd.merge(data, data_frames['STREET_DESC'], how='left', on=['USRN', 'LANGUAGE'])

    # remove non-official addresses from LPI; these can include e.g. ponds, sub stations, cctvs, public phones, etc.
    msk = data['OFFICIAL_FLAG'] == 'Y'
    data = data.loc[msk]

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


def create_random_sample_of_delivery_point_addresses(
        path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
        size=100000):
    """

    :param path:
    :param size:
    :return:
    """
    # read in delivery point table
    delivery_point = pd.read_csv(path + 'ABP_E39_DELIVERY_POINT.csv', dtype=str)
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
                      + data["BUILDING_NAME"] + ' ' + data["BUILDING_NUMBER"] + ' ' + data["THROUGHFARE"] + ' ' + \
                      data["POST_TOWN"] + ' ' + data["POSTCODE"]

    data = data[['UPRN', 'ADDRESS']]
    print('Storing single string delivery point addresses to a file...')
    data.to_csv(path + 'delivery_point_addresses.csv', index=False)


def create_final_hybrid_index(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/', filename='AB.csv',
                              output_filename='AB_processed.csv'):
    """
    A function to load an initial version of hybrid index as produced by combine_address_base_data
    and to process it to the final hybrid index used in matching.

    .. Warning: this method modifies the original AB information by e.g. combining different tables. Such
                activities are undertaken because of the aggressive blocking the prototype linking code uses.
                The actual production system should take AB as it is and the linking should not perform blocking
                but rather be flexible and take into account that in NAG the information can be stored in various
                fields.
    """
    address_base = pd.read_csv(path + filename,
                               dtype={'UPRN': np.int64, 'POSTCODE_LOCATOR': str, 'ORGANISATION_NAME': str,
                                      'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                                      'BUILDING_NUMBER': str, 'THROUGHFARE': str, 'DEPENDENT_LOCALITY': str,
                                      'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                                      'PAO_START_NUMBER': str, 'PAO_START_SUFFIX': str, 'PAO_END_NUMBER': str,
                                      'PAO_END_SUFFIX': str, 'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                                      'SAO_START_SUFFIX': str, 'ORGANISATION': str, 'STREET_DESCRIPTOR': str,
                                      'TOWN_NAME': str, 'LOCALITY': str, 'SAO_END_NUMBER': np.float64,
                                      'SAO_END_SUFFIX': str})
    print('Found {} addresses from the combined AddressBase file...'.format(len(address_base.index)))

    # remove street records from the list of potential matches - this makes the search space slightly smaller
    exclude = 'STREET RECORD|ELECTRICITY SUB STATION|PUMPING STATION|POND \d+M FROM|PUBLIC TELEPHONE|'
    exclude += 'PART OF OS PARCEL|DEMOLISHED BUILDING|CCTV CAMERA|TANK \d+M FROM|SHELTER \d+M FROM|TENNIS COURTS|'
    exclude += 'PONDS \d+M FROM|SUB STATION'
    msk = address_base['PAO_TEXT'].str.contains(exclude, na=False, case=False)
    address_base = address_base.loc[~msk]

    # combine information - could be done differently, but for now using some of these for blocking
    msk = address_base['THROUGHFARE'].isnull()
    address_base.loc[msk, 'THROUGHFARE'] = address_base.loc[msk, 'STREET_DESCRIPTOR']

    msk = address_base['BUILDING_NUMBER'].isnull()
    address_base.loc[msk, 'BUILDING_NUMBER'] = address_base.loc[msk, 'PAO_START_NUMBER']

    msk = address_base['BUILDING_NAME'].isnull()
    address_base.loc[msk, 'BUILDING_NAME'] = address_base.loc[msk, 'PAO_TEXT']

    msk = address_base['ORGANISATION_NAME'].isnull()
    address_base.loc[msk, 'ORGANISATION_NAME'] = address_base.loc[msk, 'ORGANISATION']

    msk = address_base['POSTCODE'].isnull()
    address_base.loc[msk, 'POSTCODE'] = address_base.loc[msk, 'POSTCODE_LOCATOR']

    msk = address_base['SUB_BUILDING_NAME'].isnull()
    address_base.loc[msk, 'SUB_BUILDING_NAME'] = address_base.loc[msk, 'SAO_TEXT']

    msk = address_base['POST_TOWN'].isnull()
    address_base.loc[msk, 'POST_TOWN'] = address_base.loc[msk, 'TOWN_NAME']

    msk = address_base['POSTCODE'].isnull()
    address_base.loc[msk, 'POSTCODE'] = address_base.loc[msk, 'POSTCODE_LOCATOR']

    msk = address_base['LOCALITY'].isnull()
    address_base.loc[msk, 'LOCALITY'] = address_base.loc[msk, 'DEPENDENT_LOCALITY']

    # sometimes addressbase does not have SAO_START_NUMBER even if SAO_TEXT clearly has a number
    # take the digits from SAO_TEXT and place them to SAO_START_NUMBER if this is empty
    msk = address_base['SAO_START_NUMBER'].isnull() & (address_base['SAO_TEXT'].notnull())
    address_base.loc[msk, 'SAO_START_NUMBER'] = pd.to_numeric(
        address_base.loc[msk, 'SAO_TEXT'].str.extract('(\d+)'))

    # normalise street names so that st. is always st and 's is always s
    msk = address_base['THROUGHFARE'].str.contains('ST\.\s', na=False, case=False)
    address_base.loc[msk, 'THROUGHFARE'] = address_base.loc[msk, 'THROUGHFARE'].str.replace('ST\. ', 'ST ')
    msk = address_base['THROUGHFARE'].str.contains("'S\s", na=False, case=False)
    address_base.loc[msk, 'THROUGHFARE'] = address_base.loc[msk, 'THROUGHFARE'].str.replace("'S\s", 'S ')

    # drop some that are not needed - in the future versions these might be useful
    address_base.drop(['DEPENDENT_LOCALITY', 'POSTCODE_LOCATOR', 'ORGANISATION'], axis=1, inplace=True)

    # split postcode to in and outcode - useful for doing blocking in different ways
    postcodes = address_base['POSTCODE'].str.split(' ', expand=True)
    postcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
    address_base = pd.concat([address_base, postcodes], axis=1)

    print('Using {} addresses from the final hybrid index...'.format(len(address_base.index)))

    print(address_base.info(verbose=True, memory_usage=True, null_counts=True))
    address_base.to_csv(path + output_filename, index=False)


def create_test_hybrid_index(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                             filename='AB_processed.csv', output_filename='ABtest.csv'):
    """
    Updates an existing test index to reflect changes made to the processed final hybrid index.

    :param path:
    :param filename:
    :param output_filename:

    :return: None
    """
    address_base = pd.read_csv(path + filename,
                               dtype={'UPRN': np.int64, 'POSTCODE_LOCATOR': str, 'ORGANISATION_NAME': str,
                                      'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                                      'BUILDING_NUMBER': str, 'THROUGHFARE': str, 'DEPENDENT_LOCALITY': str,
                                      'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                                      'PAO_START_NUMBER': str, 'PAO_START_SUFFIX': str, 'PAO_END_NUMBER': str,
                                      'PAO_END_SUFFIX': str, 'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                                      'SAO_START_SUFFIX': str, 'ORGANISATION': str, 'STREET_DESCRIPTOR': str,
                                      'TOWN_NAME': str, 'LOCALITY': str, 'SAO_END_NUMBER': np.float64,
                                      'SAO_END_SUFFIX': str})
    print('Found {} addresses from te hybrid index...'.format(len(address_base.index)))

    test_index_uprns = pd.read_csv(path + output_filename, usecols=['UPRN'], dtype={'UPRN': np.int64})['UPRN'].values
    print('Found {} addresses from the test index...'.format(len(test_index_uprns)))

    # find the overlap
    mask = np.in1d(address_base['UPRN'].values, test_index_uprns)

    # output to a file - overwrites the old test index
    address_base_test_index = address_base.loc[mask]
    address_base_test_index.to_csv(path + output_filename, index=False)
    print(address_base_test_index.info())


if __name__ == "__main__":
    combine_address_base_data()
    create_final_hybrid_index()
    convertCSVtoSQLite()

    create_test_hybrid_index()
