#!/usr/bin/env python
"""
ONS Address Index - Check Baseline Performance
==============================================

A simple script to run baselines using the Beta matching service.


Running
-------

After all requirements are satisfied and the _response.json files are available,
the script can be invoked using CPython interpreter::

    python check_performance.py


Requirements
------------

:requires: pandas


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 8-Feb-2017
"""
import json
import glob

import numpy as np
import pandas as pd


def _read_input_data(filename):
    """
    Read in the original data from a _minimal.csv file created by the prototype.

    :param filename: name of the CSV to read in and process
    :type filename: str

    :return: dictionary with addresses and ids
    :rtype: dict
    """
    data = pd.read_csv(filename, dtype={'ID': str, 'UPRN_prev': str, 'ADDRESS': str, 'UPRN_new': str})
    data.rename(columns={'UPRN_prev': 'UPRN_original', 'UPRN_new': 'UPRN_prototype'}, inplace=True)

    print('Input contains', len(data.index), 'addresses')

    return data


def _read_response_data(filename):
    """
    Read in the beta service response data from a stored JSON file as returned by the /bulk API end point.

    Converts id and uprn to str in case these were numeric as the dtypes for the input data are strings.

    :param filename: name of the JSON response file
    :type filename: str

    :return: response data in a tabular format with potentially multiple matches
    :rtype: pandas.DataFrame
    """
    json_data = open(filename, 'r').read()
    data = pd.read_json(json.dumps(json.loads(json_data)['resp']), orient='records')

    data['id'] = data['id'].astype(str)
    data['uprn'] = data['uprn'].astype(str)

    data.rename(columns={'uprn': 'UPRN_beta'}, inplace=True)

    return data


def _join_data(original, results, output_file):
    """
    Join the original data and the beta matching results to form a single data frame.
    Stores the data frame in CSV format to the given output file.

    :param original: the original input data with IDs and UPRNs in a data frame
    :type original: pandas.DataFrame
    :param results: the results of the beta matching service in a data frame
    :type results: pandas DataFrame
    :param output_file: name of the output file in which the joined data are stored
    :type output_file: str

    :return: joined data in a single data frame
    :rtype: pandas.DataFrame
    """
    # merge and sort by id and score, add boolean column to identify matches
    data = pd.merge(original, results, how='left', left_on='ID', right_on='id')
    data.sort_values(by=['id', 'score'], ascending=[True, False], inplace=True)
    data['matches'] = data['UPRN_original'] == data['UPRN_beta']

    data.to_csv(output_file)

    return data


def _check_performance(data):
    """
    Computes the performance on the joined data frame.

    Checks if the top ranking match is the correct one i.e. the boolean matches contains True.
    For those IDs for which the highest ranking match candidate is not the same is assumed,
    checks if the expected UPRN is found in the set of found matches. Finally, computes the number
    of correct and incorrect matches.

    :param data: joined data with beta matches and expected UPRNs
    :type data: pandas.DataFrame

    :return: None
    """
    # find the top matches for each id and check which match the input UPRN
    deduped = data.drop_duplicates(subset='id', keep='first')
    mask = deduped['matches'] == True
    correct = deduped.loc[mask]

    print('Top Ranking Match is Correct:', len(correct.index))

    # find those ids where the highest scored match is not the correct match
    top_id_is_not_correct = deduped.loc[~mask & deduped['id'].notnull()]['ID']

    if len(top_id_is_not_correct.values) > 0:
        correct_in_set = []
        incorrect = []
        for not_correct_id in top_id_is_not_correct.values:
            # check if the correct answer is within the set
            values = data.loc[data['ID'] == not_correct_id]
            sum_of_matches = np.sum(values['matches'].values)

            if sum_of_matches >= 1:
                correct_in_set.append(not_correct_id)
            else:
                incorrect.append(not_correct_id)

        print('Correct Match in the Set of Returned Addresses:', len(correct_in_set))
        print(correct_in_set)
        print('Correct Match not in the Set:', len(incorrect))
        print(incorrect)


def main(path):
    """

    :return:
    """
    response_files = glob.glob(path + '*_response.json')

    for response_file in response_files:
        print('Processing', response_file)

        address_file = response_file.replace('_response.json', '_minimal.csv')
        output_file = response_file.replace('_response.json', '_beta.csv')

        input_data = _read_input_data(address_file)
        beta_data = _read_response_data(response_file)

        results = _join_data(input_data, beta_data, output_file)

        _check_performance(results)


if __name__ == '__main__':
    main(path='/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/')
