# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 14:10:38 2017

:author: iva

script to automatically run baselines on most of the datasets during the night

requires sami's scripts run_baseline & check_performance available for import (in the same directory)

"""

from shutil import copy
import os
import time
from run_baseline import run_all_baselines
from check_performance import main as check_performance
from default_param import DEFAULT_CONFIG

tdata_path = '//tdata8/AddressIndex/Beta_Results/'
#code_path = '//tdata8/AddressIndex/Beta_Results/codes/'    #depricated, currently assumes the above import is succesful 

uri_version = 'branch'          # currently accepting strings: 'dev' or 'branch'
  # change the explanatory name !
#new_folder_name = 'April_28_branch_locality_etc'
wait_hours = 0                  # wait 5 hours before firing the queries 

#datasets=['EdgeCases',  'PatientRecords', 'LifeEvents', 'CQC',   'WelshGov2',  'WelshGov3', 'WelshGov', 'CompaniesHouse']
#datasets=['PatientRecords', 'WelshGov']#, 'CompaniesHouse']                    #or just the big ones 
#datasets=['EdgeCases',  'LifeEvents', 'WelshGov2', 'WelshGov3',  'CQC']        #or just the small ones 
datasets=['EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov']

param_values = [.3, .2, .5, 1, .1]


def main(datasets=datasets, tdata_path=tdata_path):
    print('Current time: ' + time.strftime("%H:%M:%S"))
    time.sleep(60*60*wait_hours)   
    report = dict([[dataset, 'failed'] for dataset in datasets])     
    for param_value in param_values:
      DEFAULT_CONFIG.__setitem__("fallbackPafBigramBoost", param_value)     
      DEFAULT_CONFIG.__setitem__("fallbackLpiBigramBoost", param_value) 
      for dataset in datasets:
        new_folder_name =  time.strftime("%B_%d_") +  uri_version  + '_bigrams' + str(param_value) +'_synonyms2'   
        print('Starting dataset ' + dataset + ' at ' + time.strftime("%H:%M:%S"))
        new_folder_path = tdata_path + dataset + '/'+ new_folder_name
        if not os.path.exists(new_folder_path): 
            # if the destination doesn't exist: create new folder and copy data (_minimal.csv) 
            os.makedirs(new_folder_path)            
            copy(tdata_path + 'DataSets/' + dataset + '_minimal.csv', new_folder_path)  
        try: 
            # try running the baseline scripts            
            run_all_baselines(directory = new_folder_path, uri_version=uri_version, batch_size=6000, param_config=DEFAULT_CONFIG)
            check_performance(directory = new_folder_path, ivy = True)
            report[dataset] = 'successful'
        except:
            pass
    print('Finished at ' + time.strftime("%H:%M:%S"))
    for dataset in datasets:
        print(dataset + ': ' + report[dataset])

if __name__ == '__main__':
    main()