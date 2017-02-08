#!/usr/bin/env python
"""
ONS Address Index - Run Baseline
================================

A simple script to run baselines using the Beta matching service.


Running
-------

After all requirements are satisfied and the _minimal.csv files are available,
the script can be invoked using CPython interpreter::

    python run_baseline.py


Requirements
------------

:requires: requests
:requires: pandas


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 8-Feb-2017
"""
import glob
import time

import pandas as pd
import requests


def read_data(filename):
    """
    Read in the file. Assumes that the CSV contains at least two columns named ID and ADDRESS.

    Converts the input data to a dictionary format, which can be passed on the server.
    The ideal input is the prototype _minimal.csv output file.

    :param filename: name of the CSV to read in and process
    :type filename: str

    :return: dictionary with addresses and ids
    :rtype: dict
    """
    data = pd.read_csv(filename, usecols=['ID', 'ADDRESS'], dtype={'ID': str, 'ADDRESS': str})
    data.rename(columns={'ID': 'id', 'ADDRESS': 'address'}, inplace=True)

    data = data.to_dict(orient='records')
    data = {'addresses': data}

    return data


def query_elastic(data, uri='http://addressindex-api.apps.cfnpt.ons.statistics.gov.uk:80/bulk',
                  verbose=True):
    """
    Post the given data to the given uri, which should be the API bulk endpoint.

    :param data: input addresses
    :type data: dict
    :param uri:
    :type uri: str
    :param verbose: whether or not to print out the API response
    :type verbose: bool

    :return: API response
    """
    if verbose:
        start = time.clock()
        print('Starting to execute Elastic query...')

    response = requests.post(uri, headers={"Content-Type": "application/json"}, json=data, timeout=1000000.)

    if verbose:
        stop = time.clock()
        print('Finished in {} seconds...'.format(round((stop - start), 1)))
        print(response)

    return response


def _run_baseline(filename):
    """
    Process a single CSV file, execute bulk point query, and output the response text to a file.

    :param filename: name of the CSV file to process
    :type filename: str

    :return: None
    """
    print('Processing', filename)

    data = read_data(filename)
    results = query_elastic(data)

    fh = open(filename.replace('_minimal.csv', '_response.json'), 'w')
    fh.write(results.text)
    fh.close()


def run_all_baselines():
    """
    Run baselines for all _minimal CSV files present in the working directory.

    The files are processed sequentially not to blast the ElasticSearch server with
    a large number of simultaneous queries. The execution could be done in parallel
    trivially using e.g. multiprocessing library.

    :return: None
    """
    files = glob.glob('*_minimal.csv')
    for file in files:
        _run_baseline(file)


if __name__ == '__main__':
    run_all_baselines()
