#!/usr/bin/env python
"""
ONS Address Index - Automatic Testing of Different Datasets
===========================================================

A simple wrapper to call all independent address linking datasets in serial.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python automatic_testing.py


Requirements
------------

:requires: pandas (0.19.1)
:requires: matplotlib (1.5.3)
:requires: sqlalchemy
:requires: addressLinking (and all the requirements within it)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 1-Dec-2016

"""
import os
import datetime
import Analytics.prototype.welshAddresses as wa
import Analytics.prototype.landRegistryAddresses as lr
import Analytics.prototype.edgeCaseAddresses as ec
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine


# set global local variable that is platform specific so that there is no need to make code changes
if 'Pro.local' in os.uname().nodename:
    location = '//Users/saminiemi/Projects/ONS/AddressIndex/linkedData/'
elif 'cdhut-d03-' in os.uname().nodename:
    location = '//opt/scratch/AddressIndex/Results/'
else:
    raise ConnectionError('ERROR: cannot connect to the SQLite3 database')


def run_all_datasets():
    """
    Run all address linking codes in serial.

    :return:
    """
    print('Running Welsh addresses test...')
    wa.run_welsh_address_linker()

    print('Running Landry Registry addresses test...')
    lr.run_land_registry_linker()

    print('Running Edge Case addresses test...')
    ec.run_edge_case_linker()


def compute_performance():
    """

    :return:
    """
    pass


def _get_data_from_db(sql):
    """
    Pull data from a database.

    :param sql: sql query to execute to pull the data
    :type sql: str

    :return: queried data
    :rtype: pandas.DataFrame
    """
    # build the connection string from specifying the DB type, location, and filename separately
    connection = 'sqlite://' + location + 'AddressLinkingResults.sqlite'

    df = pd.read_sql_query(sql, create_engine(connection))

    return df


def plot_performance():
    """
    Generates simple graphs which show the linking performance as a function of time for all datasets
    available from the results database.

    For each dataset two graphs are generated: 1) figure with multiple sub-figures, and 2)
    a single figure showing multiple lines.

    :return: None
    """
    # query data and place it to a Pandas DataFrame
    data = _get_data_from_db('select * from results;')

    # convert date to datetime
    data['date'] = pd.to_datetime(data['date'])

    columns_to_plot = ['addresses', 'correct', 'false_positive', 'linked', 'new_UPRNs', 'not_linked']

    # create figures
    for testset_name in set(data['name']):
        plot_data = data.loc[data['name'] == testset_name]
        print('Plotting {} results'.format(testset_name))

        plot_data.plot(x='date', y=columns_to_plot,
                       subplots=True, sharex=True, layout=(3, 2), figsize=(12, 18),
                       fontsize=16, sort_columns=True, color='m',
                       xlim=(plot_data['date'].min() - datetime.timedelta(days=1),
                             plot_data['date'].max() + datetime.timedelta(days=1)))
        plt.tight_layout()
        plt.savefig(location + testset_name + 'results.png')
        plt.close()

        plot_data.plot(x='date', y=columns_to_plot,
                       figsize=(12, 18), fontsize=16, sort_columns=True,
                       xlim=(plot_data['date'].min() - datetime.timedelta(days=1),
                             plot_data['date'].max() + datetime.timedelta(days=1)),
                       ylim=(plot_data[columns_to_plot].min(axis=0).min() - 1,
                             plot_data[columns_to_plot].max(axis=0).max() + 1))
        plt.tight_layout()
        plt.savefig(location + testset_name + 'results2.png')
        plt.close()


def run_all(plot_only=False):
    """
    Execute the full automated testing sequence.

    :param plot_only: whether to re-run all test datasets or simply generate performance figures
    :param plot_only: bool

    :return: None
    """
    if not plot_only:
        run_all_datasets()
        compute_performance()
    plot_performance()


if __name__ == "__main__":
    run_all(plot_only=True)
