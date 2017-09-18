# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 11:11:34 2017

@author: gaskk

Script to reorder the output of all datasets from Elastic to Hopper score

"""

import numpy as np
import pandas as pd
import json

# Set up file path and file names
tdata_path = '//tdata8/AddressIndex/Beta_Results/'
datasets=['EdgeCases']#, 'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov']

def add_hopper_score(data):
    """
    Takes bespoke score as output from API and extracts structural and object scores.
    Structural and object scores are basis of Hopper score.

    :param data: _beta_DedupExist.csv file which is joined data with beta matches and expected UPRNs
    :type data: pandas.DataFrame

    :return: dataframe containing structural_score and object_score columns
    :rtype: pandas.DataFrame
    """   
    
    # Add Hopper score (structural and object scores)
    bespoke_score = data['bespokeScore'].tolist()
    
    # Get structural and object scores from the bespoke score output
    struct_score = []
    obj_score = []
    for i in bespoke_score:
        json_acceptable_string = i.replace("'", "\"")
        d = json.loads(json_acceptable_string)
        struct_score.append(d['structuralScore'])
        obj_score.append(d['objectScore'])
    
    # Now add structural and object scores to the dataframe
    struct_score_df = pd.Series(struct_score)
    data['structural_score'] = struct_score_df.values
    obj_score_df = pd.Series(obj_score)
    data['object_score'] = obj_score_df.values
    
    return data

def hopper_score_duplicates(data):
    """
    Orders the input dataset in order of best Hopper score, this is structural
    then object score. Also accounts for duplicates where scores are identical
    for different addresses.

    :param data: joined data with beta matches and expected UPRNs as well as structural_score and object_score
    :type data: pandas.DataFrame

    :return: dataframe containing hopper_score (1 to 5 where 1 is best score)
    :rtype: pandas.DataFrame
    """
    
    # Sort by structural then object scores
    data.sort_values(['ID_original', 'structural_score', 'object_score'], ascending=[True, False, False])
    
    # Create hopper_score which preserves this sort into one ascending value
    data['hopper_score'] = 1
    data['hopper_score'] = data.groupby('ID_original')['hopper_score'].cumsum()
    
    # See if there are some addresses which share a top match. If so, change hopper_score to 1
    data['prev_struc_score'] = data['structural_score'].shift(1)
    data['prev_obj_score'] = data['object_score'].shift(1)
    data['hopper_score'] = np.where((data['structural_score'] == data['prev_struc_score'])
                                 & (data['object_score'] == data['prev_obj_score']) & (data['hopper_score'] == 2), 1, data['hopper_score'])
    
    return data

def check_performance_karen(data):
    """
    Computes the performance using Hopper score.

    Checks if the top ranking match is the correct one i.e. the boolean matches contains True.
    For those IDs for which the highest ranking match candidate is not the same is assumed,
    checks if the expected UPRN is found in the set of found matches.

    :param data: joined data with beta matches and expected UPRNs
    :type data: pandas.DataFrame

    :return: dataframe containing hopper_results (so whether top match, in set etc.)
    :rtype: pandas.DataFrame
    """

    data['elastic_score'] = data['score']
    data['elastic_results']=data['results']
    data['score'] = data['structural_score']*1000 + data['object_score']

    #in case of hopper score we need to filter only candidates with score >0
    msk2 = data['score'] > 0
    data['elastic_matches'] = data['matches']
    data['matches'] = data['matches'] & msk2
    data.loc[~msk2,'UPRN_beta'] = None
    
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
    data['top_match'] = data['matches'] & data['score_max'] & data['score']
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
    
    print(all_results)
    
    #switch the names back
    data['hopper_results'] = data['results']
    data['hopper_score'] = data['score']
    data['results'] = data['elastic_results']
    data['score'] = data['elastic_score']
    data['matches'] = data['elastic_matches']    
    data.drop(['score_unique','score_max','top_match','top_match_group', 'unique_top_match', 'unique_top_match_group', 'match_group', 
        'elastic_results', 'elastic_score', 'elastic_matches'], axis=1, inplace=True)
     
    return data

for dataset in datasets:
    
    # Iterate over all datasets
    dir_date = '/September_15_dev_baseline/'#/August_24_dev_hopper_score/' 
    in_filename = tdata_path + dataset + dir_date + dataset + '_beta_DedupExist.csv'
    out_filename = tdata_path + dataset + dir_date + dataset + '_hopper_score_results.csv'
    out_filename_dedup = tdata_path + dataset + dir_date + dataset + '_hopper_score_results_no_duplicates.csv'
    out_filename_pivot = tdata_path + dataset + dir_date + dataset + '_pivot_ivy.csv'
    
    # Import data
    data = pd.read_csv(in_filename, encoding='latin-1')
    
    # Run functions
    data = add_hopper_score(data)
    data = check_performance_karen(data)
    
    # Export results
    data.to_csv(out_filename)
    
    # Without duplicates ie. without five possible matched addresses, just one for summary
    data_dedup = data.sort_values(['ADDRESS', 'hopper_score', 'matches'], 
        ascending = [True, False, False], na_position='last').drop_duplicates(subset='ADDRESS', keep='first')

    #data_dedup.to_csv(out_filename_dedup)
    
    # Create pivot table comparing Elastic score and Hopper score results
    pivot = data_dedup.pivot_table(index = 'results', columns = 'hopper_results',
                                   values = 'ADDRESS', aggfunc=len)
    # Replace NaN with zeros
    pivot.fillna(0, inplace=True)
    pivot.to_csv(out_filename_pivot)
        
    print('Hopper score datasets for', dataset, 'completed')


##### Separating debug codes

def add_debug_codes(data):
    """
    Takes bespoke score as output from API and extracts all debug codes.

    :param data: any file which includes the bespokeScore
    :type data: pandas.DataFrame

    :return: dataframe containing unit_score_debug and building_score_debug columns
    :rtype: pandas.DataFrame
    """   
    
    # Add Hopper score (structural and object scores)
    bespoke_score = data['bespokeScore'].tolist()
    
    # Get structural and object scores from the bespoke score output
    unit_debug = []
    build_debug = []
    for i in bespoke_score:
        json_acceptable_string = i.replace("'", "\"")
        d = json.loads(json_acceptable_string)
        unit_debug.append(d['unitScoreDebug'])
        build_debug.append(d['buildingScoreDebug'])
    
    # Now add structural and object scores to the dataframe
    unit_debug_df = pd.Series(unit_debug)
    data['unit_score_debug'] = unit_debug_df.values
    build_debug_df = pd.Series(build_debug)
    data['building_score_debug'] = build_debug_df.values
    
    return data

# Add debug scores for Edge Cases
dataset = 'EdgeCases'
out_filename_dedup = tdata_path + dataset + '/August_24_dev_hopper_score/' + dataset + '_hopper_score_results_no_duplicates.csv'

# Import data
#data = pd.read_csv(out_filename_dedup, encoding='latin-1')
#data = add_debug_codes(data)
#data.to_csv(out_filename_dedup)