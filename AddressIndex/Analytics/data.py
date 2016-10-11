"""
ONS Address Index - Data
========================

A simple script containing methods to query or modify the ONS AddressBase data.


Requirements
------------

:requires: numpy
:requires: pandas
:requires: sqlalchemy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.6
:date: 11-Oct-2016
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import re
import glob


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
        tmp = re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]? {1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA', string)[0]
    except:
        tmp = None

    return tmp


def _getPostcode(row, column='address'):
    """
    A wrapper to getPostcode so that it can be called using dataframe.apply
    """
    return getPostcode(row[column])


def _removePostcode(row, column='address', postcode='postcode'):
    """

    :param row:
    :param column:
    :param postcode:

    :return:
    """
    return row[column].replace(row[postcode], '')


def testParsing():
    testQuery = 'SELECT address, uprn FROM addresses limit 10'
    df = queryDB(testQuery)
    df['postcode'] = df.apply(_getPostcode, axis=1)
    print(df)


def _simpleTest():
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
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/miniAB/'
    files = glob.glob(path + '*.csv')

    for file in files:
        if 'CLASSIFICATION' in file or 'SREET.csv' in file:
            pass

        tmp = pd.read_csv(file)

        if 'BLPU' in file:
            BLPU = tmp[['UPRN', 'POSTCODE_LOCATOR']]

        if 'DELIVERY_POINT' in file:
            DP = tmp[['UPRN', 'BUILDING_NUMBER', 'BUILDING_NAME', 'SUB_BUILDING_NAME',
                      'ORGANISATION_NAME', 'POSTCODE', 'POST_TOWN']]

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

    # drop if no UPRN
    data = data[np.isfinite(data['UPRN'])]

    # change uprn to int
    data['UPRN'] = data['UPRN'].astype(int)

    print(data.info())

    # drop some that are not needed
    data.drop(['POST_TOWN', 'POSTCODE', 'LANGUAGE', 'USRN'], axis=1, inplace=True)
    print(len(data.index), 'addresses')

    # save to a file
    data.to_csv(path + 'combined.csv', index=0)


def combineAddressBaseData(filename='AB.h5'):
    """
    Read in all the Address Base Epoch 39 CSV files and combine to a single HDF5 file.
    Only relevant information is retained. All information is converted to lowercase.
    Postcode is split to in and outcodes. In addition, a column listing flat number
    is added.

    :param filename: name of the output file
    :type filename: str

    :return: None
    """
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/'
    files = glob.glob(path + 'ABP_E39_*.csv')

    for file in files:
        print('\nReading file:', file)

        if 'BLPU' in file:
            BLPU = pd.read_csv(file, usecols=['UPRN', 'POSTCODE_LOCATOR'])
            print(BLPU.info())

        if 'DELIVERY_POINT' in file:
            DP = pd.read_csv(file, usecols=['UPRN', 'BUILDING_NUMBER', 'BUILDING_NAME', 'SUB_BUILDING_NAME',
                                            'ORGANISATION_NAME', 'POSTCODE', 'POST_TOWN', 'DEPARTMENT_NAME'])
            print(DP.info())

        if 'LPI' in file:
            LPI = pd.read_csv(file, usecols=['UPRN', 'USRN', 'PAO_TEXT', 'PAO_START_NUMBER', 'PAO_START_SUFFIX',
                                             'SAO_TEXT', 'SAO_START_NUMBER', 'LANGUAGE'])
            print(LPI.info())

        if 'STREET_DESC' in file:
            ST = pd.read_csv(file, usecols=['USRN', 'STREET_DESCRIPTOR', 'TOWN_NAME', 'LANGUAGE', 'LOCALITY'])
            print(ST.info())

        if 'ORGANISATION' in file:
            ORG = pd.read_csv(file, usecols=['UPRN', 'ORGANISATION'])
            print(ORG.info())

    print('\njoining the individual files...')
    data = pd.merge(BLPU, DP, how='left', on='UPRN')
    data = pd.merge(data, LPI, how='left', on='UPRN')
    data = pd.merge(data, ORG, how='left', on=['UPRN'])
    data = pd.merge(data, ST, how='left', on=['USRN', 'LANGUAGE'])

    print('dropping unnecessary information...')
    # drop if all null - there shouldn't be any...
    data.dropna(inplace=True, how='all')

    # drop some columns which are not needed
    data.drop(['POST_TOWN', 'POSTCODE', 'LANGUAGE', 'USRN'], axis=1, inplace=True)

    # drop if no UPRN - there shouldn't be any...
    data = data[np.isfinite(data['UPRN'])]

    # change uprn to int
    data['UPRN'] = data['UPRN'].astype(int)

    print('converting everything to lower case...')
    for tmp in data.columns:
        try:
            data[tmp] = data[tmp].str.lower()
        except:
            pass

    print('combining and splitting information...')
    # if SAO_TEXT is None and a value exists in SUB_BUILDING_NAME then use this
    msk = data['SAO_TEXT'].isnull()
    data.loc[msk, 'SAO_TEXT'] = data.loc[msk, 'SUB_BUILDING_NAME'].copy()

    # split flat or apartment number as separate for numerical comparison
    data['flat_number'] = None
    msk = data['SAO_TEXT'].str.contains('flat|apartment', na=False)
    data.loc[msk, 'flat_number'] = data.loc[msk, 'SAO_TEXT']
    data['flat_number'] = data.loc[msk].apply(lambda x: x['flat_number'].strip().replace('flat', '').replace('apartment', ''), axis=1)
    data['flat_number'] = pd.to_numeric(data['flat_number'], errors='coerce')

    # if SAO_TEXT or ORGANISATION is None, force it to NO, if BUILDING_NAME is None then set it to NONAME
    data.loc[data['SAO_TEXT'].isnull(), 'SAO_TEXT'] = 'NO'
    data.loc[data['ORGANISATION'].isnull(), 'ORGANISATION'] = 'NO'
    data.loc[data['BUILDING_NAME'].isnull(), 'BUILDING_NAME'] = 'NONAME'

    # change column names
    data.rename(columns={'POSTCODE_LOCATOR': 'postcode', 'STREET_DESCRIPTOR': 'street_descriptor',
                         'TOWN_NAME': 'town_name', 'BUILDING_NUMBER': 'building_number', 'PAO_TEXT': 'pao_text',
                         'SAO_TEXT': 'sao_text', 'BUILDING_NAME': 'building_name', 'LOCALITY': 'locality'}, inplace=True)

    print('splitting the postcode to in and out...')
    pcodes = data['postcode'].str.split(' ', expand=True)
    pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
    data = pd.concat([data, pcodes], axis=1)
    print(data.info())
    print(len(data.index), 'addresses in the combined file')

    print('storing to a CSV file...')
    data.to_csv(path + filename.replace('.h5', '.csv'), index=False)

    print('converting object types to string types - HDF5 does not like storing python objects...')
    for col in data.columns:
        if data[col].dtype == object:
            data[col] = data[col].astype(str)
    print(data.info())

    print('storing to a HDF5 file...')
    data.to_hdf(path + filename, 'data', format='table', mode='w', dropna=True)


def processPostcodeFile():
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/old/'
    df = pd.read_csv(path + 'postcodefile.csv')

    b = pd.DataFrame(df.PostcodeDistricts.str.split(',').tolist(), index=df.PostTown).stack()
    b = b.reset_index()[[0, 'PostTown']]  # var1 variable is currently labeled 0
    b.columns = ['PostTown', 'PostcodeDistricts']  # renaming var1

    #lower case and stripping
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


if __name__ == "__main__":
    # _simpleTest()
    # testParsing()
    # processPostcodeFile()
    # combineMiniABtestingData()
    # combineAddressBaseData()
    modifyEdgeCasesData()