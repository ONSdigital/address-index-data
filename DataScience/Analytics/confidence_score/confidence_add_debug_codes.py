# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 11:37:44 2018

:author: iva 

script to automatically extract debug codes and hopper score from baseline datasets
"""

import numpy as np
import pandas as pd
import json
import time

# Set up file path and file names
tdata_path = '//tdata8/AddressIndex/Beta_Results/'
datasets=['EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov']
new_folder_name = 'May_15_test_threshold0'
file_ext_in = '_beta_DedupExist.csv'
file_ext_out = '_debug_codes.csv'

##### Separating debug codes

def add_all_debug_codes(data):
    """
    Takes bespoke score as output from API and extracts all debug codes.

    :param data: need to contain a column bespokeScore from which we extract all json fields
    :type data: pandas.DataFrame

    :return: dataframe containing inpu data + columns with fields from bespokeScore
    :rtype: pandas.DataFrame
    """   
    
    # extract bespoke score as jason
    bespoke_score = data['bespokeScore'].apply(lambda x: json.loads(x.replace("'", "\"")))
    
    # convert list of json into a dataframe
    bespoke_df = pd.read_json(bespoke_score.to_json(orient='index'),orient='index')
    
    # add the scores to the data (as dataframes)
    data2 = pd.concat([data, bespoke_df], axis=1)
    
    return data2


def main(datasets=datasets, new_folder_name= new_folder_name, tdata_path=tdata_path):
    print('Current time: ' + time.strftime("%H:%M:%S"))
    report = dict([[dataset, 'failed'] for dataset in datasets]) 
    for dataset in datasets:
        print('Starting dataset ' + dataset + ' at ' + time.strftime("%H:%M:%S"))
        new_folder_path = tdata_path + dataset + '/'+ new_folder_name +'/'
        try: 
            # try to attach the debug codes to the baseline data            
            data = pd.read_csv(new_folder_path + dataset + file_ext_in, encoding='latin-1')
            data = add_all_debug_codes(data)
            data.to_csv(new_folder_path + dataset + file_ext_out, index=False)
            report[dataset] = 'successful'
        except:
            pass
    print('Finished at ' + time.strftime("%H:%M:%S"))
    for dataset in datasets:
        print(dataset + ': ' + report[dataset])
        

if __name__ == '__main__':
    main()
