#!/usr/bin/env python
"""
ONS Address Index - Create Training Data for the Probabilistic Parser
=====================================================================

A simple script to use AddressBase information to create training and houldout
samples for probabilistic parser.

The script allows to reformat the delivery point table information so
that it can be used to train a probabilistic parser. The training data
need to be in XML format and each address token specified separately.
The holdout data is drawn from the same underlying data source, but it
is independent from the training sample, that is there are no addresses
that appear in both samples.


Requirements
------------

:requires: pandas
:requires: numpy


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python createTrainingDataProbabilisticParser.py



Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.6
:date: 6-Feb-2017
"""
import numbers
import numpy as np
import pandas as pd

# set the random seed so that we get the same training and holdout data even if rerun
np.random.seed(seed=42)


def _toXML(row):
    """
    Convert pandas DataFrame row to string that is valid XML and can be used
    for training. Assumes that the data frame columns are in the appropriate
    order.

    :param row: pandas data frame row containing the address information

    :return: a string containing an address in tokenised format
    :rtype: str
    """
    xml = ['\n   <AddressString>']

    for field in row.index:
        tmp = None
        if row[field] is not None:
            # test for the type and then proceed accordingly
            if isinstance(row[field], str):
                # string
                if 'nan' not in row[field]:
                    tmp = row[field]
            elif isinstance(row[field], numbers.Number):
                # number
                if np.isfinite(row[field]):
                    tmp = row[field]
            else:
                print('ERROR:', row[field], type(row[field]))

            if tmp is not None:
                # test if it can be split by space, need multiple components
                try:
                    t = tmp.split(' ')
                    for component in t:
                        xml.append('<{0}>{1}</{0}> '.format(field, component))
                except:
                    xml.append('<{0}>{1}</{0}> '.format(field, tmp))

    xml[-1] = xml[-1][:-1]
    xml.append('</AddressString>')

    return ''.join(xml)


def create_training_data_from_delivery_point_table(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                                                   filename='ABP_E39_DELIVERY_POINT.csv',
                                                   training_sample_size=10000000, holdout_sample_size=100000,
                                                   training_subsamples=(1000000, 100000, 10000, 1000),
                                                   out_path='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/',
                                                   outfile='training10M.xml', holdout_file='holdout.xml'):
    """
    Create training and holdout files for the probabilistic parser. Takes a pandas DataFrame
    as an input, re-orders the information, splits it to training and holdout data, and finally
    outputs to two XML files.

    The output is in the following format:
        <AddressCollection>
            <AddressString><label>token</label> <label>token</label> <label>token</label></AddressString>
            <AddressString><label>token</label> <label>token</label></AddressString>
        </AddressCollection>

    :param path: location of the address base data
    :type path: str
    :param filename: name of the address base PAF file
    :type filename: str
    :param training_sample_size: number of training samples, if exceeds the number of examples then no holdout data
    :type training_sample_size: int
    :param holdout_sample_size: number of holdout samples, if exceeds the number of potential holdouts, then use all
    :type holdout_sample_size: int
    :param training_subsamples: a tuple of containing the number of samples to use for subsamples drawn
                                from the training data
    :type training_subsamples: tuple
    :param out_path: location where to store the output files
    :type out_path: str
    :param outfile: name of the training data file
    :type outfile: str
    :param holdout_file: name of the holdout data file
    :type holdout_file: str

    :return: None
    """
    columns = {'ORGANISATION_NAME': 'OrganisationName',
               'DEPARTMENT_NAME': 'DepartmentName',
               'SUB_BUILDING_NAME': 'SubBuildingName',
               'BUILDING_NAME': 'BuildingName',
               'BUILDING_NUMBER': 'BuildingNumber',
               'THROUGHFARE': 'StreetName',
               'DEPENDENT_LOCALITY': 'Locality',
               'POST_TOWN': 'TownName',
               'POSTCODE': 'Postcode'}

    data = pd.read_csv(path + filename, dtype=str, usecols=columns.values())

    print('Renaming columns...')
    data.rename(columns=columns, inplace=True)

    print('re-ordering the columns to match the order expected in an address...')
    neworder = ['OrganisationName',
                'DepartmentName',
                'SubBuildingName',
                'BuildingName',
                'BuildingNumber',
                'StreetName',
                'Locality',
                'TownName',
                'Postcode']
    data = data[neworder]
    print(data.info())

    print('Initial length', len(data.index))
    print('remove those with STREET RECORD, PARKING SPACE, or POND in BuildingName...')
    msk = data['BuildingName'].str.contains('STREET RECORD|PARKING SPACE|POND', na=False)
    data = data.loc[~msk]
    print('After removing parking spaces etc.', len(data.index))

    print('changing ampersands to AND...')
    for col in data.columns.values.tolist():
        data[col] = data[col].str.replace(r'&', 'AND', case=False)

    print('Deriving training and holdout data...')
    if len(data.index) > training_sample_size:
        rows = np.random.choice(data.index.values, training_sample_size)
        msk = np.in1d(data.index.values, rows)
        training = data.loc[msk]
        holdout = data.loc[~msk]
        if len(holdout.index) > holdout_sample_size:
            holdout = holdout.sample(n=holdout_sample_size)
    else:
        print('ERROR: only', len(data.index), 'addresses, using all for training!')
        training = data
        holdout = None

    print('Removing 5 per cent of postcodes and mushing 5 per cent together')
    rows = np.random.choice(training.index.values, int(training_sample_size * 0.05))
    msk = np.in1d(training.index.values, rows)
    training.loc[msk, 'Postcode'] = training.loc[msk, 'Postcode'].str.replace(' ', '')
    rows = np.random.choice(training.index.values, int(training_sample_size * 0.05))
    msk = np.in1d(training.index.values, rows)
    training.loc[msk, 'Postcode'] = None

    # todo: maybe on should drop randomly also building number or street name from some addresses?

    print('writing full training data to an XML file...')
    fh = open(out_path + outfile, mode='w')
    fh.write('<AddressCollection>')
    fh.write(''.join(training.apply(_toXML, axis=1)))
    fh.write('\n</AddressCollection>')
    fh.close()

    print('writing the holdout data to an XML file...')
    if holdout is not None:
        fh = open(out_path + holdout_file, mode='w')
        fh.write('<AddressCollection>')
        fh.write(''.join(holdout.apply(_toXML, axis=1)))
        fh.write('\n</AddressCollection>')
        fh.close()

    # take smaller samples - useful for testing the impact of training data
    for sample_size in training_subsamples:
        print('Drawing randomly', sample_size, 'samples from the training data...')
        sample = training.sample(n=sample_size)
        print('writing small training data of', sample_size, 'to an XML file...')
        fh = open(out_path + outfile.replace('10M', str(sample_size)), mode='w')
        fh.write('<AddressCollection>')
        fh.write(''.join(sample.apply(_toXML, axis=1)))
        fh.write('\n</AddressCollection>')
        fh.close()


if __name__ == "__main__":
    create_training_data_from_delivery_point_table()
