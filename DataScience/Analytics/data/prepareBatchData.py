"""
ONS Address Index - Convert Prototype Data to Batch Input Format
================================================================

A simple script to convert prototype minimal output to batch input JSON format.


Requirements
------------

:requires: pandas


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 7-Feb-2017
"""
import glob
import os

import pandas as pd


def _read_data(filename):
    """
    Read in data from a _minimal.csv file and rename columns.

    :param filename: name of the input _minimal.csv file

    :return: data
    :rtype: pandas.DataFrame
    """
    data = pd.read_csv(filename, usecols=['ID', 'ADDRESS'], dtype={'ID': str, 'ADDRESS': str})
    data.rename(columns={'ID': 'id', 'ADDRESS': 'address'}, inplace=True)

    return data


def _save_to_json_file(data, filename):
    """
    Save the given data to a JSON file.

    :param data: data to be stored to JSON format
    :type data: pandas.DataFrame
    :param filename: name of the output file
    :type filename: str

    :return: None
    """
    tempfile = filename.replace('.json', '.tmp')

    data.to_json(tempfile, orient='records')

    fh = open(tempfile)
    out = open(filename, 'w')

    out.write('{"addresses":')
    for line in fh.readlines():
        out.write(line)
    out.write('}')

    fh.close()
    out.close()

    # remove the tmp file
    os.remove(tempfile)


def process_all_minimal_files():
    """
    Process all _minimal.csv files and save to json for baseline batch processing.

    :return: None
    """
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/'

    files = glob.glob(path + '*_minimal.csv')

    for file in files:
        print('Processing', file)
        data = _read_data(file)
        _save_to_json_file(data, file.replace('_minimal.csv', '.json'))


if __name__ == '__main__':
    process_all_minimal_files()
