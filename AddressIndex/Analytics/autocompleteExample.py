"""
ONS Address Index - AutoComplete Test
=====================================

A simple ...


Requirements
------------

:requires: pandas
:requires: sqlalchemy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 27-Sep-2016
"""
import autocomplete
from autocomplete import models
from AddressIndex.Analytics import data


def getData():
    testQuery = 'SELECT address FROM addresses'
    # pull data from db
    df = data.queryDB(testQuery)
    # convert to a python list
    lst = df['address'].values.flatten()
    # conver to a string
    string = ''.join(lst)

    return string


def trainModel(text):
    models.train_models(text)


def runServer():
    autocomplete.run_server()


def runAll():
    string = getData()
    trainModel(string)
    runServer()

if __name__ == "__main__":
    runAll()






