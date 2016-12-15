#!/usr/bin/env python
"""
ONS Address Index - Companies House Performance
===============================================

A simple script to compute the matching performance for the companies house data provided by the business index.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python companiesHousePerformance.py


Requirements
------------

:requires: pandas (0.19.1)
:requires: matplotlib (tested with 1.5.3)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 15-Dec-2016
"""
import pandas as pd
import matplotlib.pyplot as plt


def load_matching_results(filename='/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/CompaniesHouse_matched.csv'):
    """
    Load the matching results of the companies house data.

    :param filename: name of the CSV file storing matches
    :type filename: str

    :return: matching results for the business index companies house data
    :rtype: pandas.DataFrame
    """
    data = pd.read_csv(filename, usecols=['ID', 'UPRN', 'ADDRESS'])

    data['CH_ID'] = data['ID'].str.replace('_CH', '')
    data['CCEW_ID'] = data['ID'].str.replace('_CCEW', '')

    print('{} matches found'.format(len(data.index)))

    return data


def compute_performance(data):
    """
    Check the number of UPRNs found for the two data sources that match.

    :param data: data frame containing matching results and ID columns for the two data sources
    :type data: pandas.DataFrame

    :return: results containing a column named Match with boolean flag for match/non-match
    :rtype: pandas.DataFrame
    """
    results = pd.merge(data, data, how='inner', left_on='CH_ID', right_on='CCEW_ID', suffixes=('_CH', '_CCEW'))

    results['Match'] = results['UPRN_CH'] == results['UPRN_CCEW']

    print('Total number of entries is {}'.format(len(results.index)))
    print('UPRNs match {}'.format(results['Match'].sum()))

    results.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/CompaniesHouse_performance.csv',
                   index=False)

    return results


def visualise_performance(data):
    """
    A simple histogram visualisation summarising the number of matches and non-matches.

    :param data: results containing a column named Match with boolean flag for match/non-match
    :type data: pandas.DataFrame

    :return: None
    """
    data.plot(y='Match', kind='hist', xlim=(-0.5, 1.5), xticks=(0, 1))
    plt.tight_layout()
    plt.savefig('/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/CompaniesHouse_performance.png')
    plt.close()


def run_all():
    """
    Run the complain chain from loading data to a simple visualisation.

    :return: None
    """
    data = load_matching_results()
    results = compute_performance(data)
    visualise_performance(results)


if __name__ == "__main__":
    run_all()
