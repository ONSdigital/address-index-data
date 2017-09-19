# -*- coding: utf-8 -*-
"""
Created on Jul-Sep 2017

@author: gaskk & ivyONS

Script to reorder the output of all datasets from Elastic to Confidence score

"""

import numpy as np
import pandas as pd
import json

# Set up file path and file names
tdata_path = '//tdata8/AddressIndex/Beta_Results/'
datasets=['EdgeCases', 'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov']

def add_confidence_score(data):
    """
    Takes bespoke score as output from API and extracts structural and object scores.
    Structural and object scores are basis of Confidence score.

    :param data: _beta_DedupExist.csv file which is joined data with beta matches and expected UPRNs
    :type data: pandas.DataFrame

    :return: dataframe containing structural_score and object_score columns
    :rtype: pandas.DataFrame
    """   
    
    # Add Confidence score (structural and object scores)
    bespoke_score = data['bespokeScore'].tolist()
    
    # Get structural and object scores from the bespoke score output
    struct_score = []
    obj_score = []
    unit_debug = []
    build_debug = []
    local_debug = []
    for i in bespoke_score:
        json_acceptable_string = i.replace("'", "\"")
        d = json.loads(json_acceptable_string)
        struct_score.append(d['structuralScore'])
        obj_score.append(d['objectScore'])
        unit_debug.append(d['unitScoreDebug'])
        build_debug.append(d['buildingScoreDebug'])
        local_debug.append(d['localityScoreDebug'])
    
    # Now add structural and object scores to the dataframe
    struct_score_df = pd.Series(struct_score)
    data['structural_score'] = struct_score_df.values
    obj_score_df = pd.Series(obj_score)
    data['object_score'] = obj_score_df.values
    unit_debug_df = pd.Series(unit_debug)
    data['unit_score_debug'] = unit_debug_df.values
    build_debug_df = pd.Series(build_debug)
    data['building_score_debug'] = build_debug_df.values
    local_debug_df = pd.Series(local_debug)
    data['locality_score_debug'] = local_debug_df.values
    
    return data

def check_performance_karen(data, remove0s = True ):
    """
    Computes the performance using Confidence score.

    Checks if the top ranking match is the correct one i.e. the boolean matches contains True.
    For those IDs for which the highest ranking match candidate is not the same is assumed,
    checks if the expected UPRN is found in the set of found matches.

    :param data: joined data with beta matches and expected UPRNs
    :type data: pandas.DataFrame

    :return: dataframe containing confidence_results (so whether top match, in set etc.)
    :rtype: pandas.DataFrame
    """

    data['elastic_score'] = data['score']
    data['elastic_results']=data['results']
    data['score'] = data['structural_score']*1000 + data['object_score']

    #in case of confidence score we need to filter only candidates with score >0
    #here (not nice) we just rename the variables and then swith them back
    data['elastic_matches'] = data['matches']
    data['elastic_UPRN_beta'] =data['UPRN_beta']
    if (remove0s):
      msk2 = data['score'] > 0
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
    data['confidence_results'] = data['results']
    data['confidence_score'] = data['score']
    data['results'] = data['elastic_results']
    data['score'] = data['elastic_score']
    data['matches'] = data['elastic_matches'] 
    data['UPRN_beta'] = data['elastic_UPRN_beta']
    data.drop(['score_unique','score_max','top_match','top_match_group', 'unique_top_match', 'unique_top_match_group', 'match_group', 
        'elastic_results', 'elastic_score', 'elastic_matches', 'elastic_UPRN_beta'], axis=1, inplace=True)
     
    return data

for dataset in datasets:
    
    # Iterate over all datasets
    dir_date = '/September_15_dev_baseline/'#/August_24_dev_confidence_score/' 
    in_filename = tdata_path + dataset + dir_date + dataset + '_beta_DedupExist.csv'
    out_filename = tdata_path + dataset + dir_date + dataset + '_confidence_score_results.csv'
    out_filename_dedup = tdata_path + dataset + dir_date + dataset + '_confidence_score_results_no_duplicates.csv'
    out_filename_pivot = tdata_path + dataset + dir_date + dataset + '_pivot_ivy.csv'
    
    # Import data
    data = pd.read_csv(in_filename, encoding='latin-1')
    
    # Run functions
    data = add_confidence_score(data)
    data = check_performance_karen(data, remove0s = True)
    
    # Export results
    #data.to_csv(out_filename)
    
    # Without duplicates ie. without five possible matched addresses, just one for summary
    data_dedup = data.sort_values(['ADDRESS', 'confidence_score', 'UPRN_beta', 'matches',  'UPRN_comparison'], 
        ascending =  [True, False, True, False, True], na_position='last').drop_duplicates(subset='ADDRESS', keep='first')

    #data_dedup.to_csv(out_filename_dedup)
    
    # Create pivot table comparing Elastic score and Confidence score results
    pivot = data_dedup.pivot_table(index = 'results', columns = 'confidence_results',
                                   values = 'ADDRESS', aggfunc=len)
    # Replace NaN with zeros
    pivot.fillna(0, inplace=True)
    pivot.to_csv(out_filename_pivot)
        
    print('Confidence score datasets for', dataset, 'completed')