#!/usr/bin/env python
"""
ONS Address Index - Extracting SAO Information
==============================================

A simple script to check the logic for extracting SAO information from an input string.


Running
-------

When all requirements are satisfied the script can be run from command line using CPython::

    python extractSAOinformation.py


Requirements
------------

:requires: pandas
:requires: ProbabilisticParser (a CRF model specifically build for ONS)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 28-Feb-2017
"""
import pandas as pd
from Analytics.linking import addressParser


def read_data(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/', filename='SAO_END_SUFFIX.xlsx'):
    """
    Read in the data containing addresses with both PAO_START_NUMBER and SAO_END_SUFFIX present.

    :param path: location of the input file
    :type path: str
    :param filename: name of the input file
    :type filename: str

    :return: input data in a single data frame
    :rtype: pandas.DataFrame
    """
    df = pd.read_excel(path + filename)
    return df


def parse_SAO_information(path, output):
    """
    Parse SAO information from a special file containing addresses with both PAO_START_NUMBER and SAO_END_SUFFIX
    populated.

    :param path: location to which the output will be stored
    :type path: str
    :param output: name of the output file
    :type output: str

    :return: None
    """
    input_data = read_data()

    address_parser = addressParser.AddressParser()
    output_data = address_parser.parse(input_data)

    output_data.to_csv(path + output)


if __name__ == "__main__":
    parse_SAO_information('/Users/saminiemi/Projects/ONS/AddressIndex/data/', 'SAO_END_SUFFIX_parsed.csv')
