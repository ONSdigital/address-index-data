"""
ONS Address Index - Data Access Test
====================================

A simple ...


Requirements
------------

:requires: pandas
:requires: sqlalchemy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 27-Sep-2016
"""
import pandas as pd
from sqlalchemy import create_engine
import re


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


def testParsing():
    testQuery = 'SELECT address FROM addresses limit 10'
    df = queryDB(testQuery)
    df['Postcode'] = df.apply(_getPostcode, axis=1)
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


if __name__ == "__main__":
    # _simpleTest()
    testParsing()