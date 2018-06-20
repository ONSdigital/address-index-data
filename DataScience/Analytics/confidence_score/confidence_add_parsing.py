# -*- coding: utf-8 -*-
"""
Created April 2018

:author:  iva 

Attaching parsing confidence to the datasets for input into machine learning algorithm for confidence score
"""

# Import libraries
import pandas as pd
import time
import os

tdata_path = '//tdata8/AddressIndex/Beta_Results/'
datasets=['EdgeCases', 'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov']
new_folder_name = 'May_15_test_threshold0'
file_ext_in = '_ML_features_all5.csv'
file_ext_out = '_ML_features_5_parse.csv'

#load the CRF suite and trained model    
retval = os.getcwd()
os.chdir( "H:\\My Documents\\python\\address-index-data\\DataScience" )
import ProbabilisticParser.parser as parser
os.chdir( retval )
def get_parseprob (address):
    return parser.parse_with_probabilities(address)['sequence_probability']

def add_parsing_score(data):
    data['parsing_score'] = data['ADDRESS'].apply(get_parseprob)
    return data


def main(datasets=datasets, new_folder_name= new_folder_name, tdata_path=tdata_path):
    print('Current time: ' + time.strftime("%H:%M:%S"))
    report = dict([[dataset, 'failed'] for dataset in datasets]) 
    for dataset in datasets:
        print('Starting dataset ' + dataset + ' at ' + time.strftime("%H:%M:%S"))
        new_folder_path = tdata_path + dataset + '/'+ new_folder_name +'/'
        try: 
            # try to attach the debug codes to the baseline data            
            data = pd.read_csv(new_folder_path + dataset + file_ext_in, encoding='latin-1')
            data = add_parsing_score(data)
            data.to_csv(new_folder_path + dataset + file_ext_out, index=False)
            report[dataset] = 'successful'
        except:
            pass
    print('Finished at ' + time.strftime("%H:%M:%S"))
    for dataset in datasets:
        print(dataset + ': ' + report[dataset])
        

if __name__ == '__main__':
    main()
