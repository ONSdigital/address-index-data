#!/usr/bin/env python
"""
ONS Address Index - Data Exploration
====================================

A simple script to perform quick data exploration on the ONS AddressBase data.


Running
-------

When all requirements are satisfied and AddressBase is available,
the script can be run from command line using CPython::

    python addressBaseExploration.py


Requirements
------------

:requires: pandas
:requires: pandas-profiling


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 21-Feb-2017
"""
import pandas as pd
import pandas_profiling


def perform_data_exploration(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/', filename='AB.csv'):
    """
    Perform simple data exploration using pandas-profiling. Outputs the results to an HTML file.

    :param path: path of the input information to profile
    :type path: str
    :param filename: name of the file containing the data
    :type filename: str

    :return: None
    """
    df = pd.read_csv(path + filename, encoding='UTF-8')

    print(df.info())

    pfr = pandas_profiling.ProfileReport(df)
    pfr.to_file(path + 'AddressBase.html')


if __name__ == "__main__":
    perform_data_exploration()
