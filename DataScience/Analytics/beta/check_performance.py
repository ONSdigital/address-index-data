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
:requires: numpy
:requires: matplotlib


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.3
:date: 14-Feb-2017
"""
import datetime
import glob
import json
import sys

import matplotlib.pyplot as plt
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

    if ('UPRN_prev' in data) and ('UPRN_new' in data):
        data.rename(columns={'UPRN_prev': 'UPRN_comparison', 'UPRN_new': 'UPRN_prototype'}, inplace=True)
    elif 'UPRN_prev' in data:
        data.rename(columns={'UPRN_prev': 'UPRN_comparison'}, inplace=True)
    elif 'UPRN_new' in data:
        data.rename(columns={'UPRN_new': 'UPRN_comparison'}, inplace=True)
    else:
        print('No comparison UPRNs available, will exit')
        sys.exit(-9)

    data.rename(columns={'ID': 'ID_original'}, inplace=True)

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
    data = pd.read_csv(filename)

    data['id'] = data['id'].astype(str)
    data['uprn'] = data['uprn'].astype(str)

    data.rename(columns={'uprn': 'UPRN_beta', 'id': 'id_response'}, inplace=True)

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
    data = pd.merge(original, results, how='left', left_on='ID_original', right_on='id_response')
    data.sort_values(by=['id_response', 'score'], ascending=[True, False], inplace=True)
    data['matches'] = data['UPRN_comparison'] == data['UPRN_beta']

    data.reset_index(inplace=True)

    data.to_csv(output_file, index=False)

    return data


def _check_performance(data, verbose=True):
    """
    Computes the performance on the joined data frame.

    Checks if the top ranking match is the correct one i.e. the boolean matches contains True.
    For those IDs for which the highest ranking match candidate is not the same is assumed,
    checks if the expected UPRN is found in the set of found matches. Finally, computes the number
    of correct and incorrect matches.

    :param data: joined data with beta matches and expected UPRNs
    :type data: pandas.DataFrame
    :param verbose: whether or not to output the partial and non-matche IDs
    :type verbose: bool

    :return: None
    """
    results = []

    # all addresses with unique original id -- assumes uniqueness
    number_of_entries = data['ID_original'].nunique()
    results.append(number_of_entries)
    print('Input addresses (unique IDs):', number_of_entries)

    # find those that were not matched
    msk = data['UPRN_beta'].isnull()
    not_matched = data.loc[msk, 'ID_original'].nunique()
    results.append(not_matched)
    print('Not matched:', not_matched)

    # find the top matches for each id and check which match the input UPRN
    deduped = data.copy().drop_duplicates(subset='id_response', keep='first')
    msk = deduped['matches'] == True
    correct = deduped.loc[msk]
    number_of_correct = len(correct.index)

    print('Top Ranking Match is Correct:', number_of_correct)
    results.append(number_of_correct)

    # find those ids where the highest scored match is not the correct match and UPRNs were found
    top_id_is_not_correct = deduped.loc[~msk & deduped['UPRN_beta'].notnull()]['ID_original']

    correct_in_set = []
    incorrect = []
    if len(top_id_is_not_correct.values) > 0:
        for not_correct_id in top_id_is_not_correct.values:
            # check if the correct answer is within the set
            values = data.loc[data['ID_original'] == not_correct_id]
            sum_of_matches = np.sum(values['matches'].values)

            if sum_of_matches >= 1:
                correct_in_set.append(not_correct_id)
            else:
                incorrect.append(not_correct_id)

        print('Correct Match in the Set of Returned Addresses:', len(correct_in_set))
        print('Correct Match not in the Set:', len(incorrect))

    results.append(len(correct_in_set))
    results.append(len(incorrect))

    if verbose:
        print(correct_in_set)
        print(incorrect)

    return results


def _generate_performance_figure(all_results, filename, width=0.35):
    """
    Generate a simple bar chart to show the results.

    :param all_results: a list containing all the results as computed by the _check_performance method
    :type all_results: list
    :param filename: name of the output file
    :type filename: str
    :param width: fractional width of the bars
    :type width: float

    :return: None
    """
    all_results_names = ['Input Addresses', 'Not Matched', 'Top Ranking Match', 'Within the Set', 'Incorrect']
    location = np.arange(len(all_results))

    fig = plt.figure(figsize=(12, 10))
    plt.title('Beta Linking Service ({})'.format(datetime.datetime.now().strftime("%Y-%m-%d %H%M%S")))
    ax = fig.add_subplot(1, 1, 1)

    max_bar_length = max(all_results)
    plt.barh(location, all_results, width, color='g', alpha=0.6)

    for patch in ax.patches:
        if patch.get_x() < 0:
            continue

        n_addresses = int(patch.get_width())
        ratio = n_addresses / max_bar_length

        if ratio > 0.3:
            ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                        xytext=(-95, 18), textcoords='offset points', color='white', fontsize=24)
        else:
            ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                        xytext=(10, 18), textcoords='offset points', color='black', fontsize=24)

    plt.xlabel('Number of Addresses')
    plt.yticks(location, all_results_names)
    plt.xlim(0, ax.get_xlim()[1] * 1.02)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


def main(path):
    """
    Execute all steps needed to read in original data, beta response, join the tables and finally
    to compute simple performance numbers and to generate a simple bar chart.

    :return:
    """
    response_files = glob.glob(path + '*_response.csv')

    for response_file in response_files:
        print('Processing', response_file)

        address_file = response_file.replace('_response.csv', '_minimal.csv')
        output_file = response_file.replace('_response.csv', '_beta.csv')
        output_figure_file = response_file.replace('_response.csv', '_performance.png')

        input_data = _read_input_data(address_file)
        beta_data = _read_response_data(response_file)

        results = _join_data(input_data, beta_data, output_file)

        results = _check_performance(results)

        _generate_performance_figure(results, output_figure_file)
        # todo: add computation of presision, recall, and f1 score


if __name__ == '__main__':
    main(path='/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/')
