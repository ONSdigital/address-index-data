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

:version: 0.4
:date: 15-Feb-2017
"""
import datetime
import glob
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _read_input_data(filename, use_prototype=False):
    """
    Read in the original data from a _minimal.csv file created by the prototype.

    :param filename: name of the CSV to read in and process
    :type filename: str
    :param use_prototype: whether or not to use the prototype UPRNs
    :type use_prototype: bool

    :return: dataframe containing the input data used for the Beta request
    :rtype: pandas.DataFrame
    """
    data = pd.read_csv(filename, dtype={'ID': str, 'UPRN_prev': np.float64, 'ADDRESS': str, 'UPRN_new': np.float64})

    if use_prototype:
        if 'UPRN_new' in data:
            data.rename(columns={'UPRN_new': 'UPRN_comparison'}, inplace=True)
        else:
            print('Prototype UPRNs not found, will skip...')
            return None
    else:
        if ('UPRN_prev' in data) and ('UPRN_new' in data):
            data.rename(columns={'UPRN_prev': 'UPRN_comparison', 'UPRN_new': 'UPRN_prototype'}, inplace=True)
        elif 'UPRN_prev' in data:
            data.rename(columns={'UPRN_prev': 'UPRN_comparison'}, inplace=True)
        else:
            print('No comparison UPRNs available, will skip...')
            return None

    data.rename(columns={'ID': 'ID_original'}, inplace=True)

    print('Input contains', len(data.index), 'addresses')

    return data


def _read_response_data(filename, low_memory =False):
    """
    Read in the beta service response data from a stored JSON file as returned by the /bulk API end point.

    Converts id and uprn to str in case these were numeric as the dtypes for the input data are strings.

    :param filename: name of the JSON response file
    :type filename: str

    :return: response data in a tabular format with potentially multiple matches
    :rtype: pandas.DataFrame
    """
    if low_memory:
         data = pd.read_csv(filename, low_memory=True,  
                            usecols=['id', 'score', 'uprn'], 
                            dtype={'id': str, 'score': np.float64, 'uprn': np.float64})
    else:
         data = pd.read_csv(filename, low_memory=False)

    data['id'] = data['id'].astype(str)
    data['uprn'] = data['uprn'].astype(np.float64)

    data.rename(columns={'uprn': 'UPRN_beta', 'id': 'id_response'}, inplace=True)

    return data


def _join_data(original, results, output_file=''):
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
    data.sort_values(by=['ID_original', 'score'], ascending=[True, False], inplace=True)
    data['matches'] = data['UPRN_comparison'] == data['UPRN_beta']

    data.reset_index(inplace=True)
    
    if output_file != '':
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

    # drop duplicates to check how many were matched and for how many of the highest ranking match is correct
    #deduped_original = data.copy().drop_duplicates(subset='ADDRESS', keep='first')    
    deduped_original = data.copy()
    # Create a data frame that includes the scores and add a column with the frequency of each row.
    deduped_original['top_counts'] = deduped_original.groupby(['ID_original', 'score'])['score'].transform('count')
    # Drop the duplicates so that there is only one entry for each address (if there is choice keep the one with existing UPRN).
    deduped_original = deduped_original.sort_values(['ADDRESS', 'score', 'UPRN_beta', 'matches',  'UPRN_comparison'], 
        ascending = [True, False, True, False, True], na_position='last').drop_duplicates(subset='ADDRESS', keep='first')
        
    number_of_entries = len(deduped_original['UPRN_comparison'].index)
    results.append(number_of_entries)
    print('Input addresses (unique IDs):', number_of_entries)

    # how many entries have existing UPRN in data
    msk = deduped_original['UPRN_comparison'].notnull()
    existing_uprns_count = len(deduped_original.loc[msk].index)
    print(existing_uprns_count, 'of the input addresses have UPRNs attached')
    results.append(existing_uprns_count)

    # find those that were not matched and remove from the data
    msk = deduped_original['UPRN_beta'].isnull()
    not_matched_count = len(deduped_original.loc[msk].index)
    print('Not matched:', not_matched_count)
    deduped_original = deduped_original.loc[~msk]

    # find those without existing UPRN but with new beta one found and then remove them
    msk = deduped_original['UPRN_comparison'].isnull()
    new_uprns_count = len(deduped_original.loc[msk].index)
    print('New UPRNs:', new_uprns_count)
    deduped_original = deduped_original.loc[~msk]

    # find those where the top scoring UPRN_beta matches original and it is UNIQUE (strictly larger score than the second)
    msk = deduped_original['top_counts'] == 1 & deduped_original['matches']
    number_of_correct = len(deduped_original.loc[msk].index)
    print('Top Ranking Match is Correct:', number_of_correct)
    results.append(number_of_correct)

    # find those ids where the highest scored match is not the correct match
    top_id_is_not_correct = deduped_original.loc[~msk, 'ID_original']
    print('Top ranking is incorrect or non unique:', len(top_id_is_not_correct.index))

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
    results.append(new_uprns_count)
    results.append(len(incorrect))
    results.append(not_matched_count)

    if verbose:
        print(correct_in_set)
        print(incorrect)

    return results

def _check_performance_ivy(data, verbose=True,  output_file=''):
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

    data['results'] = ''

    # classifications that only need to look at one line at a time:
    msk0 = (data['UPRN_comparison'].notnull()) #| (data['UPRN_comparison'] =='')
    msk1 = (data['UPRN_beta'].isnull()) #| (data['UPRN_beta'] =='')
    data.loc[msk1&msk0, 'results'] = '3_not_found'
    data.loc[~msk1&~msk0, 'results'] = '5_new_uprn'
    data.loc[msk1&~msk0, 'results'] = '6_both_missing'
    data.loc[~msk1&msk0, 'results'] = '4_wrong_match'  # default value - correct matches will be overwritten
    
    # classifications where we need to look at all candidates at the same time:
    # add helper columns and then combine them to create appropriate filters
    data['score_unique'] = data.groupby(['ID_original', 'score'])['score'].transform('count') == 1
    data['score_max'] = data.groupby(['ID_original'])['score'].transform('max') == data['score']
    data['top_match'] = data['matches'] & data['score_max']
    data['top_match_group'] = data.groupby(['ID_original'])['top_match'].transform('sum')>0
    data['unique_top_match'] = data['top_match'] & data['score_unique']
    data['unique_top_match_group'] = data.groupby(['ID_original'])['unique_top_match'].transform('sum')>0
    data['match_group'] = data.groupby(['ID_original'])['matches'].transform('sum')>0

    data.loc[data['match_group'],'results'] = '2_in_set_lower'
    data.loc[data['top_match_group'],'results'] = '2_in_set_equal'
    data.loc[data['unique_top_match_group'],'results'] = '1_top_unique'

    # Drop the duplicates so that there is only one entry for each address (if there is choice keep the one with existing UPRN).
    deduped_original = data.copy()
    deduped_original = deduped_original.sort_values(['ADDRESS', 'score', 'UPRN_beta', 'matches',  'UPRN_comparison'], 
        ascending = [True, False, True, False, True], na_position='last').drop_duplicates(subset='ADDRESS', keep='first')
        
    results = deduped_original.results.value_counts().sort_index().to_dict()

    result_names = [ '1_top_unique','2_in_set_equal', '2_in_set_lower', '3_not_found', '4_wrong_match','5_new_uprn', '6_both_missing']
    all_results = pd.Series([results.get(key,0) for key in result_names])
    all_results.index = result_names   

    if verbose:
        print(all_results)
    
    if output_file != '': 
        data.drop(['score_unique','score_max','top_match','top_match_group', 'unique_top_match', 'unique_top_match_group', 'match_group'], axis=1, inplace=True)
        data.to_csv(output_file, index=False)

    return all_results


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
    all_results_names = ['Input Addresses', 'Existing UPRNs', 'Top Ranking Match', 'In the Set', 'New UPRNs',
                         'Different UPRNs', 'Not Matched']
    location = np.arange(len(all_results))
    
    fig = plt.figure(figsize=(12, 10))
    plt.title('Matching performance ' + os.path.dirname(os.path.splitdrive(filename)[1]))# 'Beta Address Linking ({})'.format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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
                        xytext=(-95, 8), textcoords='offset points', color='white', fontsize=24)
        else:
            ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                        xytext=(10, 8), textcoords='offset points', color='black', fontsize=24)
                        
    plt.xlabel('Number of Addresses')
    plt.yticks(location, all_results_names)
    plt.xlim(0, ax.get_xlim()[1] * 1.02)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    
def _generate_performance_figure_ivy(all_results, filename, width=0.35):
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
    #results = [all_results[2], all_results[3], all_results[1]-all_results[2]-all_results[3]-all_results[5], all_results[5], 
    #           all_results[1], all_results[4], all_results[0]-all_results[1]-all_results[4], all_results[0]-all_results[1]]    
    all_results_names = [ '1 Top match', '2 In the set', '3 Not found', '4 Wrong match', "Input with UPRNs",
                          "5 New uprn", "6 Both missing", "Input without UPRNs" ]
    results = [all_results[0], all_results[1]+all_results[2], all_results[3], all_results[4],
               sum(all_results[0:5]), all_results[5], all_results[6],  all_results[5] + all_results[6]]                    
    results.reverse()
    all_results_names.reverse()   
    all_results = results        
                 
    location = np.arange(len(all_results))
    
    fig = plt.figure(figsize=(12, 10))
    plt.title('Matching performance ' + os.path.dirname(os.path.splitdrive(filename)[1]))
    ax = fig.add_subplot(1, 1, 1)
    
    max_bar_length = max(all_results)
    plt.barh(location, all_results, width, color=['b', 'r', 'g','b', 'r', 'r', 'g', 'g'], alpha=0.6)
    
    for patch in ax.patches:
        if patch.get_x() < 0:
            continue
        
        n_addresses = int(patch.get_width())
        ratio = n_addresses / max_bar_length
        
        if ratio > 0.3:
            ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                        xytext=(-95, 8), textcoords='offset points', color='white', fontsize=24)
        else:
            ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                        xytext=(10, 8), textcoords='offset points', color='black', fontsize=24)
                        
    plt.xlabel('Number of Addresses')
    plt.yticks(location, all_results_names)
    plt.xlim(0, ax.get_xlim()[1] * 1.02)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()    
    

def main(directory = os.getcwd(), ivy=True):
    """
    Execute all steps needed to read in original data, beta response, join the tables and finally
    to compute simple performance numbers and to generate a simple bar chart.

    :return:
    """
    response_files = glob.glob(directory + '\\*_response.csv')
    for response_file in response_files:
        print('Processing', response_file)
        address_file = response_file.replace('_response.csv', '_minimal.csv')

        for name in [('DedupExist')]: #('Existing', 'Prototype'):
            print('Calculating Performance using', name, 'UPRNs')
            output_figure_file = response_file.replace('_response.csv', '_performance_' + name + '.png')
            output_file = response_file.replace('_response.csv', '_beta_' + name + '.csv')     

            if 'Prototype' in name:
                input_data = _read_input_data(address_file, use_prototype=True)
            else:
                input_data = _read_input_data(address_file)

            if input_data is not None:
                if input_data.shape[0] > 200000:        # note that we won't read in the tokens, matched address and other fields
                    beta_data = _read_response_data(response_file, low_memory=True)
                else:
                    beta_data = _read_response_data(response_file, low_memory=False)
               
                if ivy:
                    joined_data = _join_data(input_data, beta_data, output_file = '')
                    del(input_data, beta_data)
                    results = _check_performance_ivy(joined_data, output_file = output_file)
                    _generate_performance_figure_ivy(results, response_file.replace('_response.csv', '_performance_' + name + '2.png'))
                else:
                   joined_data = _join_data(input_data, beta_data, output_file)
                   del(input_data, beta_data)
                   results = _check_performance(joined_data)
                   _generate_performance_figure(results, output_figure_file)   


if __name__ == '__main__':
    main()
