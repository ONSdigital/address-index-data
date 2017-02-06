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
seed = np.random.seed(seed=42)
# turn of pandas chaining warning
pd.options.mode.chained_assignment = None


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
                except ValueError:
                    xml.append('<{0}>{1}</{0}> '.format(field, tmp))

    xml[-1] = xml[-1][:-1]
    xml.append('</AddressString>')

    return ''.join(xml)


def _remove_fraction_of_labels(data, label, sample_total_size, fraction=0.05):
    """
    Remove a given fraction of the entries in a given label. Does not check whether
    the label contains an entry or is Null.

    :param data: input data
    :type data: pandas.DataFrame
    :param label: name of the data frame column
    :type label: str
    :param sample_total_size: size of the dataset
    :type sample_total_size: int
    :param fraction: fraction of the sample size to remove
    :type fraction: float

    :return: input data but a given fraction of the inputs set to None
    :rtype: pandas.DataFrame
    """
    print('Dropping', fraction * 100., 'per cent of', label, 'entries')

    rows = np.random.choice(data.index.values, int(sample_total_size * fraction))
    msk = np.in1d(data.index.values, rows)

    data.loc[msk, label] = None

    return data


def create_training_data_from_delivery_point_table(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                                                   filename='ABP_E39_DELIVERY_POINT.csv',
                                                   training_sample_size=1000000, holdout_sample_size=100000,
                                                   training_subsamples=(100000, 10000, 1000),
                                                   out_path='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/',
                                                   outfile='training1M.xml', holdout_file='holdout.xml'):
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
    # delivery point information used for training a probabilistic parser
    columns = {'ORGANISATION_NAME': 'OrganisationName',
               'DEPARTMENT_NAME': 'DepartmentName',
               'SUB_BUILDING_NAME': 'SubBuildingName',
               'BUILDING_NAME': 'BuildingName',
               'BUILDING_NUMBER': 'BuildingNumber',
               'THROUGHFARE': 'StreetName',
               'DEPENDENT_LOCALITY': 'Locality',
               'POST_TOWN': 'TownName',
               'POSTCODE': 'Postcode',
               'WELSH_THOROUGHFARE': 'WelshStreetName',
               'WELSH_DEPENDENT_LOCALITY': 'WelshLocality',
               'WELSH_POST_TOWN': 'WelshTownName',
               'PO_BOX_NUMBER': 'PObox'}

    data = pd.read_csv(path + filename, dtype=str, usecols=columns.keys())

    print('Data source contains', len(data.index), 'addresses')
    print('removing STREET RECORDs, PARKING SPACEs, PONDs etc...')
    # remove street records from the list of potential matches - this makes the search space slightly smaller
    exclude = 'STREET RECORD|ELECTRICITY SUB STATION|PUMPING STATION|POND \d+M FROM|PUBLIC TELEPHONE|'
    exclude += 'PART OF OS PARCEL|DEMOLISHED BUILDING|CCTV CAMERA|TANK \d+M FROM|SHELTER \d+M FROM|TENNIS COURTS|'
    exclude += 'PONDS \d+M FROM|SUB STATION|PARKING SPACE'
    msk = data['BUILDING_NAME'].str.contains(exclude, na=False, case=False) | \
          data['THROUGHFARE'].str.contains(exclude, na=False, case=False)
    data = data.loc[~msk]
    print('After removing parking spaces etc.', len(data.index), 'addresses remain')

    # remove PO BOXes
    # todo: confirm what to do with PO BOXes - separate process of part of the parser? Latter -> new label
    msk = data['PO_BOX_NUMBER'].isnull()
    data = data.loc[msk]
    print('After removing PO boxes', len(data.index), 'addresses remain')

    print('Replacing English spelling with Welsh names for those addresses where Welsh is available')
    msk = data['WELSH_THOROUGHFARE'].notnull() & data['WELSH_DEPENDENT_LOCALITY'].notnull() & \
          data['WELSH_POST_TOWN'].notnull()
    data.loc[msk, 'THROUGHFARE'] = data.loc[msk, 'WELSH_THOROUGHFARE']
    data.loc[msk, 'DEPENDENT_LOCALITY'] = data.loc[msk, 'WELSH_DEPENDENT_LOCALITY']
    data.loc[msk, 'POST_TOWN'] = data.loc[msk, 'WELSH_POST_TOWN']
    print(len(data.loc[msk].index), 'addresses with Welsh spelled street name')

    # set up sampling weight for Welsh addresses
    data['WELSH_weights'] = 1.
    data.loc[msk, 'WELSH_weights'] = 5.

    # down sample to the required size and reset the index
    total_sample_size = training_sample_size + holdout_sample_size
    if len(data.index) > total_sample_size:
        data = data.sample(n=total_sample_size, weights='WELSH_weights', random_state=seed)
    else:
        print('ERROR: sum of training and holdout sample sizes exceeds the data size')
    data.reset_index(inplace=True)
    index_values = data.index.values

    print('Mushing 5 per cent of postcodes together by removing the white space between in and outcodes')
    random_rows = np.random.choice(index_values, size=int(total_sample_size * 0.05), replace=False)
    msk = np.in1d(data.index.values, random_rows, assume_unique=True)
    data.loc[msk, 'POSTCODE'] = data.loc[msk, 'POSTCODE'].str.replace(' ', '')

    data = _remove_fraction_of_labels(data, 'POSTCODE', total_sample_size, fraction=0.05)

    data = _remove_fraction_of_labels(data, 'THROUGHFARE', total_sample_size, fraction=0.05)

    data = _remove_fraction_of_labels(data, 'POST_TOWN', total_sample_size, fraction=0.05)

    data = _remove_fraction_of_labels(data, 'BUILDING_NAME', total_sample_size, fraction=0.05)

    print('\nRenaming and re-ordering the columns to match the order expected in an address...')
    data.rename(columns=columns, inplace=True)

    new_order = ['OrganisationName',
                 'DepartmentName',
                 'SubBuildingName',
                 'BuildingName',
                 'BuildingNumber',
                 'StreetName',
                 'Locality',
                 'TownName',
                 'Postcode']
    data = data[new_order]
    print(data.info())

    print('changing ampersands to AND...')  # if not done, will create problems in the training phase
    for col in data.columns.values.tolist():
        data[col] = data[col].str.replace(r'&', 'AND', case=False)

    print('\nDeriving training and holdout samples...')
    random_rows = np.random.choice(index_values, size=training_sample_size, replace=False)
    msk = np.in1d(index_values, random_rows, assume_unique=True)
    training = data.loc[msk]
    holdout = data.loc[np.invert(msk)]
    print('Training data:')
    print(training.info())
    print('Holdout data:')
    print(holdout.info())

    print('\nWriting full training data to an XML file...')
    fh = open(out_path + outfile, mode='w')
    fh.write('<AddressCollection>')
    fh.write(''.join(training.apply(_toXML, axis=1)))
    fh.write('\n</AddressCollection>')
    fh.close()

    print('Writing the holdout data to an XML file...')
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
        fh = open(out_path + outfile.replace('1M', str(sample_size)), mode='w')
        fh.write('<AddressCollection>')
        fh.write(''.join(sample.apply(_toXML, axis=1)))
        fh.write('\n</AddressCollection>')
        fh.close()


if __name__ == "__main__":
    create_training_data_from_delivery_point_table()
