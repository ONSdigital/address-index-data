"""
ONS Address Index - Data Access Test
====================================

A simple script...


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


if __name__ == "__main__":
    testQuery = 'SELECT COUNT(*) FROM abp_blpu'
    df = queryDB(testQuery)
    print(df)

    testQuery = 'SELECT * FROM abp_delivery_point limit 10'
    df = queryDB(testQuery)
    print(df)
