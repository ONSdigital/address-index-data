"""
ONS Address Index - Run Baseline
================================

A simple script to t


Requirements
------------

:requires: requests


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 7-Feb-2017
"""
import pandas as pd
import requests
import json


def read_data(filename):
    """

    :param filename:
    :return:
    """
    data = pd.read_csv(filename, )



def query_elastic(data, uri='https://addressindex-api.apps.cfnpt.ons.statistics.gov.uk:80/bulk'):
    """

    :param data:
    :param uri:
    :return:
    """
    response = requests.post(uri, headers={"Content-Type": "application/json"}, json=json.dumps(data))

    return response


def run_baseline(filename):
    data = read_data(filename)
    results = query_elastic(data)

    print(results)

    fh = open(filename.replace('.json', '_response.json'), 'w')
    fh.write(results)
    fh.close()

if __name__ == '__main__':
    run_baseline('EdgeCases_minimal.csv')
