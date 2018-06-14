# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 11:37:44 2018

:author: karen & iva 

 Data wrangling for input into machine learning algorithm for confidence score
"""

# Import libraries
import pandas as pd
import json
import re
import numpy as np
import time

tdata_path = '//tdata8/AddressIndex/Beta_Results/'
datasets=['EdgeCases', 'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov']
new_folder_name = 'May_15_test_threshold0'
file_ext_in = '_debug_codes.csv'
file_ext_out = '_ML_features_all5.csv'
    
def add_tokens(data):  
    """
    Takes tokens as output from API and extracts all debug codes.

    :param data: need to conatin a column bespokeScore from which we extract all json fields
    :type data: pandas.DataFrame

    :return: dataframe containing inpu data + columns with fields from bespokeScore
    :rtype: pandas.DataFrame
    """   
    
    # extract tokens as json (use regular extression to get rid of names with apostrofe - likely to cause problems with very weird addresses)
    tokens = data['tokens'].apply(lambda x: re.sub('\'', '"',re.sub(r'"([^"\'\,]*\')+[^"\'\,]*"', '\'WRONG QUOTES\'',x)))
    tokens = tokens.apply(json.loads)
    
    # convert list of json into a dataframe
    tokens_df = pd.read_json(tokens.to_json(orient='index'),orient='index')
  
    # replace the actual values with only boolean indicators
    tokens_df = tokens_df.isnull().applymap(lambda x: 1-x)
    tokens_df['tokens_number'] = tokens_df.sum(axis=1)
    
    # add the scores to the data (as dataframes)
    data2 = pd.concat([data, tokens_df], axis=1)
    
    return data2


def split_debug_codes(df):
    """
    Splits debug codes (localityScoreDebug, buildingScoreDebug and unitScoreDebug)
    into separate debug codes

    :param df: _beta_DedupExist.csv file which contains column names
    localityScoreDebug, buildingScoreDebug and unitScoreDebug, or output from
    add_hopper_score function
    :type df: pandas.DataFrame

    :return: dataframe containing extra columns for each debug code
    :rtype: pandas.DataFrame
    """
    
    def get_digit(number, position = 0):
         return np.floor(number/(10**position)) % 10
    
    # Locality score debug: [(ORGANISATION_BUILDING_NAME value), (STREET value), 
    # (TOWN_LOCALITY value), (POSTCODE value)]
    df['org_build_debug'] = get_digit(df['localityScoreDebug'], 3)
    df['street_debug'] = get_digit(df['localityScoreDebug'], 2)
    df['town_locality_debug'] = get_digit(df['localityScoreDebug'], 1)
    df['postcode_debug'] = get_digit(df['localityScoreDebug'], 0)
    
    # Building score debug: [(DETAILED_ORGANISATION_BUILDING_NAME value),
    # (BUILDING_NUMBER value)]
    df['det_org_build_debug'] = get_digit(df['buildingScoreDebug'], 1)
    df['building_number_debug'] = get_digit(df['buildingScoreDebug'], 0)
    
    # Unit score debug: [(ref_HIERARCHY value), (ORGANISATION_NAME value),
    # (SUB_BUILDING_NAME value), (SUB-BUILDING_NUMBER value)]
    df['hierarchy_debug'] = get_digit(df['unitScoreDebug'], 3)
    df['org_name_debug'] =  get_digit(df['unitScoreDebug'], 2)
    df['sub_building_name_debug'] =  get_digit(df['unitScoreDebug'], 1)
    df['sub_building_num_debug'] =  get_digit(df['unitScoreDebug'], 0)
        
    return df



def normalise_elastic_scores(df, var_name='e_score', new_var_name = 'e_score'):
    """
    Create a normalised scores: This is the difference between the top Elastic
    score and the score of the closest competitor. So if top Elastic scorer
    gets 2, second scorer 1, last three scorers 0.5, should show normalised
    scores of 0.5, -0.5, -1, -1, -1
    
    :param df: _beta_DedupExist.csv file which contains column names
    ID_original and scores
    :type df: pandas.DataFrame

    :return: dataframe containing extra columns normalised_e_score
    :rtype: pandas.DataFrame 
    """
        
    # First sort data by ID_original then score
    df1 = df.sort_values(['ID_original', var_name, 'UPRN_beta'], ascending=[True, False, True],  inplace=False)
    
    # Average Elastic score between top and second top scorer by ID_original
    
    # Get top two scores in each group
    top2 = df1.groupby('ID_original').head(2).reset_index()
    
    # Mean of score by ID_original (if unique take 3/4 of it instead)
    count2 = top2.groupby(['ID_original'])[var_name].transform('count')
    mean_score = top2.groupby(['ID_original'])[var_name].transform('mean')/4*(2+count2)
    top2['mean_score'] = mean_score - .00000001
    
    # Merge with DataFrame to get the normalised scores
    df1 = pd.merge(df, top2[['ID_original','mean_score']].drop_duplicates(subset='ID_original'), on=['ID_original'])
    df1[new_var_name +'_diff'] = df1[var_name] - df1['mean_score']
    df1[new_var_name +'_ratio'] = df1[var_name] / df1['mean_score']
    del df1['mean_score']
    
    return df1

def add_simple_confidence(df, e_sigmoid = lambda x: 1/(1+np.exp(15*(.99-x))) , h_power = lambda x: x**6):
    """
    Attach simple confidence to the data frame: This is calculated by taking maximum of 
    sigmoid of elastic score and power of hopper score.
    
    :param df: _beta_DedupExist.csv file which contains column names
    ID_original and scores
    :type df: pandas.DataFrame

    :return: dataframe containing extra columns normalised_e_score
    :rtype: pandas.DataFrame 
    """
        
    # First combine stuff into hopper
    for columnname in ['SubBuildingName', 'SaoStartNumber', 'OrganisationName']:
        if not df.columns.contains(columnname):
            df[columnname] = 0
    df['h_alpha'] = np.where(df['SubBuildingName']+df['SaoStartNumber']+df['OrganisationName']>0, 
      .8, .9)
    df['unitScore2'] = np.where((df['unitScoreDebug']==999) | (df['unitScoreDebug']==1999), 
      .2, df['unitScore']*(df['unitScore']>0))
   
    df['h_score'] = df['structuralScore']*( df['h_alpha'] +(.99- df['h_alpha'])*df['unitScore2'])
    df['h_power'] = h_power(df['h_score'])
    df['e_sigmoid'] = e_sigmoid(df['e_score_ratio'])
    df['simple_confidence'] = (99*df[['h_power', 'e_sigmoid']].max(axis=1)+df['e_sigmoid'])/100
    
    return df

def karens_features(data):
    edge = data.loc[~data['UPRN_comparison'].isnull()]
    edge = add_tokens(edge)
    edge = split_debug_codes(edge)

    edge['e_score'] = edge['elasticScore']  
    edge = normalise_elastic_scores(edge)
    edge = add_simple_confidence(edge)
    
    ## Next tidy up all columns - remove some, convert others to categorical
    ## for inputting into a logistic regression / random forests algorithm
    
    # Length of address field
    edge['address_length'] = edge['ADDRESS'].apply(lambda x: len(x))
    
    # Drop multiple columns
    edge.drop([ 'UPRN_prototype', 'id_response', 'inputAddress',  'tokens',
                'bespokeScore', 'elasticScore'], axis=1, inplace=True)
    
    # Keep only two cases per ID_original. This will ensure that the
    # dataset is roughly balanced between matches=TRUE and matches=FALSE
    #edge2 = edge.groupby('ID_original').head(2)
    
    # Finally output data for input into machine learning algorithm
    return edge#2


def main(datasets=datasets, new_folder_name= new_folder_name, tdata_path=tdata_path):
    print('Current time: ' + time.strftime("%H:%M:%S"))
    report = dict([[dataset, 'failed'] for dataset in datasets]) 
    for dataset in datasets:
        print('Starting dataset ' + dataset + ' at ' + time.strftime("%H:%M:%S"))
        new_folder_path = tdata_path + dataset + '/'+ new_folder_name +'/'
        try: 
            # try to attach the debug codes to the baseline data            
            data = pd.read_csv(new_folder_path + dataset + file_ext_in, encoding='latin-1')
            data = karens_features(data)
            data.to_csv(new_folder_path + dataset + file_ext_out, index=False)
            report[dataset] = 'successful'
        except:
            pass
    print('Finished at ' + time.strftime("%H:%M:%S"))
    for dataset in datasets:
        print(dataset + ': ' + report[dataset])
        

if __name__ == '__main__':
    main()