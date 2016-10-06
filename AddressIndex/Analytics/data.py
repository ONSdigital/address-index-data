"""
ONS Address Index - Data
========================

A simple script contaning methods to query or modify the ONS AddressBase data.


Requirements
------------

:requires: pandas
:requires: sqlalchemy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.3
:date: 5-Oct-2016
"""
import pandas as pd
from sqlalchemy import create_engine
import re
import glob


def queryDB(sql, connection='postgresql://postgres@localhost/ONSAI'):
    """
    Query PostGres ONS AI database.

    :param sql: query to be executed
    :type sql: str
    :param connection: defition of the connection over which to qeury
    :type connection: str

    :return: results of the query in a pandas dataframe
    :rtype: pandas dataframe

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
    return row[column].rstrip(row[postcode])


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

    # join the various dataframes
    data = pd.merge(LPI, DP, how='left', on='UPRN')
    data = pd.merge(data, BLPU, how='left', on='UPRN')
    data = pd.merge(data, ST, how='left', on=['USRN', 'LANGUAGE'])

    # drop some that are not needed
    data.drop(['POST_TOWN', 'POSTCODE', 'LANGUAGE', 'USRN'], axis=1, inplace=True)
    print(len(data.index), 'addresses')

    # drop if all null
    data.dropna(inplace=True, how='all')

    # save to a file
    data.to_csv(path + 'combined.csv', index=0)


if __name__ == "__main__":
    # _simpleTest()
    # testParsing()
    combineMiniABtestingData()