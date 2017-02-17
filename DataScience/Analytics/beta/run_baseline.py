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
:requires: numpy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 14-Feb-2017
"""
import glob
import time

import numpy as np
import pandas as pd
import requests


def read_data(filename):
    """
    Read in the file. Assumes that the CSV contains at least two columns named ID and ADDRESS.

    The ideal input is the prototype _minimal.csv output file.

    :param filename: name of the CSV to read in and process
    :type filename: str

    :return: a data frame with with addresses and ids
    :rtype: pandas.DataFrame
    """
    data = pd.read_csv(filename, usecols=['ID', 'ADDRESS'], dtype={'ID': str, 'ADDRESS': str})
    data.rename(columns={'ID': 'id', 'ADDRESS': 'address'}, inplace=True)

    return data


def query_elastic(data, uri='http://addressindex-api.apps.cfnpt.ons.statistics.gov.uk:80/bulk',
                  verbose=True):
    """
    Post the given data to the given uri, which should be the API bulk endpoint.

    Converts the input data frame to dictionary format and posts to the uri.

    :param data: data frame containing input addresses
    :type data: pandas.DataFrame
    :param uri:
    :type uri: str
    :param verbose: whether or not to print out the API response
    :type verbose: bool

    :return: API response
    """
    data = data.to_dict(orient='records')
    data = {'addresses': data}

    if verbose:
        start = time.clock()
        print('Starting to execute Elastic query...')

    response = requests.post(uri, headers={"Content-Type": "application/json"}, json=data, timeout=10000000.)

    if verbose:
        stop = time.clock()
        print('Finished in {} seconds...'.format(round((stop - start), 1)))
        print(response)

    return response


def _create_chunks(data, batch_size=1000):
    """
    Creates arrays of roughly equal size from input data frames.

    :param data: pandas data frame that need to be split to roughly equal size chunks
    :type data: pandas.DataFrame
    :param batch_size: approximate size of the requested chunk
    :type batch_size: int

    :return: array of roughly equal size data frames
    :rtype: np.ndarray
    """
    splits = int(len(data.index) / batch_size)
    chunks = np.array_split(data, splits)

    return chunks


def _run_baseline(filename, mini_batch=True, batch_size=1000):
    """
    Process a single CSV file, execute bulk point query, and output the response text to a file.

    :param filename: name of the CSV file to process
    :type filename: str
    :param mini_batch: whether to chunk the queries to batches

    :return: None
    """
    print('Processing', filename)

    data = read_data(filename)

    if mini_batch:
        data_chunks = _create_chunks(data, batch_size=batch_size)
        results = []

        for i, data_chunk in enumerate(data_chunks):
            response = query_elastic(data_chunk)

            try:
                fh = open(filename.replace('_minimal.csv', '_response_chunk{}.json'.format(i)), mode='wb')
                fh.write(response.text.encode('utf-8'))
                fh.close()
            except ValueError:
                print('Chunk', i, 'has not return text')
                print(response)

            try:
                results.append(response.json()['resp'])
            except ConnectionError:
                print('Chunk', i, 'failed')
                print(response)

        data_frames = [pd.DataFrame.from_dict(result) for result in results]
        data_frame = pd.concat(data_frames)
    else:
        results = query_elastic(data).json()['resp']
        data_frame = pd.DataFrame.from_dict(results)

    data_frame.to_csv(filename.replace('_minimal.csv', '_response.csv'), index=False, encoding='utf-8')


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
