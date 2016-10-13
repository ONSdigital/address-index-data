"""
ONS Address Index - Create Training Data for the Probabilistic Parser
=====================================================================

A simple script to combine AddressBase information and to reformat it so
that it can be used to train a probabilistic parser. The training data
need to be in XML format and each address token specified separately.


Requirements
------------

:requires: pandas
:requires: numpy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 13-Oct-2016
"""
import pandas as pd
import numpy as np
import glob
import numbers


def combineAddressBaseData(path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                           outpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/',
                           filename='ABforTraining.csv', nrows=None):
    """
    Read in all the Address Base Epoch 39 CSV files and combine to a single CSV file.

    :param path: location of the AddressBase files
    :type path: str
    :param outpath: location where to store a temporary CSV output file
    :type outpath: str
    :param filename: name of the output file
    :type filename: str
    :param nrows: number of rows to read from the AddressBase files (default=None=All)
    :type nrows: int or None

    :return: combined data in a single dataframe
    :rtype: pandas.DataFrame
    """
    files = glob.glob(path + 'ABP_E39_*.csv')

    for file in files:
        print('\nReading file:', file)

        if 'BLPU' in file:
            BLPU = pd.read_csv(file, usecols=['UPRN', 'POSTCODE_LOCATOR'], nrows=nrows, dtype=str)
            print(BLPU.info())

        if 'DELIVERY_POINT' in file:
            DP = pd.read_csv(file, usecols=['UPRN', 'BUILDING_NUMBER', 'BUILDING_NAME', 'SUB_BUILDING_NAME',
                                            'ORGANISATION_NAME', 'POSTCODE', 'POST_TOWN', 'DEPARTMENT_NAME'],
                             nrows=nrows, dtype=str)
            print(DP.info())

        if 'LPI' in file:
            LPI = pd.read_csv(file, usecols=['UPRN', 'USRN', 'PAO_TEXT', 'PAO_START_NUMBER', 'PAO_START_SUFFIX',
                                             'SAO_TEXT', 'SAO_START_NUMBER', 'LANGUAGE'], nrows=nrows, dtype=str)
            print(LPI.info())

        if 'STREET_DESC' in file:
            ST = pd.read_csv(file, usecols=['USRN', 'STREET_DESCRIPTOR', 'TOWN_NAME', 'LANGUAGE', 'LOCALITY'],
                             nrows=nrows, dtype=str)
            print(ST.info())

        if 'ORGANISATION' in file:
            ORG = pd.read_csv(file, usecols=['UPRN', 'ORGANISATION'], nrows=nrows, dtype=str)
            print(ORG.info())

    print('\njoining the individual files...')
    data = pd.merge(BLPU, DP, how='left', on='UPRN')
    data = pd.merge(data, LPI, how='left', on='UPRN')
    data = pd.merge(data, ORG, how='left', on=['UPRN'])
    data = pd.merge(data, ST, how='left', on=['USRN', 'LANGUAGE'])

    print('dropping unnecessary information...')
    # drop if all null - there shouldn't be any...
    data.dropna(inplace=True, how='all')

    # drop some columns which are not needed
    data.drop(['POST_TOWN', 'POSTCODE', 'LANGUAGE', 'USRN'], axis=1, inplace=True)

    # change uprn to int
    data['UPRN'] = data['UPRN'].astype(int)

    # drop if no UPRN - there shouldn't be any...
    data = data[np.isfinite(data['UPRN'])]

    # combine BUILDING_NUMBER and LPI.PAO_START_NUMBER
    msk = data['BUILDING_NUMBER'].isnull()
    data.loc[msk, 'BUILDING_NUMBER'] = data.loc[msk, 'PAO_START_NUMBER']
    # combine BUILDING_NAME and PAO_TEXT
    msk = data['BUILDING_NAME'].isnull()
    data.loc[msk, 'BUILDING_NAME'] = data.loc[msk, 'PAO_TEXT']
    # combine ORGANISATION_NAME and ORGANISATION
    msk = data['ORGANISATION_NAME'].isnull()
    data.loc[msk, 'ORGANISATION_NAME'] = data.loc[msk, 'ORGANISATION']

    # drop those that been combined with other columns
    data.drop(['PAO_START_NUMBER', 'PAO_TEXT', 'ORGANISATION'], axis=1, inplace=True)

    # rename columns to match the probabilistic parser definitions
    data.rename(columns={'POSTCODE_LOCATOR': 'Postcode',
                         'STREET_DESCRIPTOR': 'StreetName',
                         'TOWN_NAME': 'TownName',
                         'BUILDING_NUMBER': 'BuildingNumber',
                         'SUB_BUILDING_NAME': 'SubBuildingName',
                         'ORGANISATION_NAME': 'OrganisationName',
                         'BUILDING_NAME': 'BuildingName',
                         'DEPARTMENT_NAME': 'DepartmentName',
                         'PAO_START_SUFFIX': 'BuildingNumberSuffix',
                         'SAO_TEXT': 'SubBuildingPrefix',
                         'SAO_START_NUMBER': 'SubBuildingNumber',
                         'LOCALITY': 'Locality'}, inplace=True)

    print(data.info())
    print(len(data.index), 'addresses in the combined file')

    print('storing to a CSV file...')
    data.to_csv(outpath + filename, index=False)

    return data


def _toXML(row):
    """
    Convert pandas dataframe row to string that is valid XML and can be used
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
                xml.append('<{0}>{1}</{0}> '.format(field, tmp))

    xml[-1] = xml[-1][:-1]
    xml.append('</AddressString>')

    return ''.join(xml)


def createTrainingData(data, trainingsize=5,
                       outpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/',
                       outfile='training.xml', holdoutfile='holdout.xml'):
    """
    Create training and holdout files for the probabilistic parser. Takes a pandas dataframe
    as an input, re-orders the information, splits it to training and holdout data, and finally
    outputs to two XML files.

    The output is in the following format:
        <AddressCollection>
            <AddressString><label>token</label> <label>token</label> <label>token</label></AddressString>
            <AddressString><label>token</label> <label>token</label></AddressString>
        </AddressCollection>

    :param data: pandas dataframe containing the addresses in tokenised format
    :type data: pandas.DataFrame
    :param trainingsize: number of training samples, if exceeds the number of examples then no holdout data
    :type trainingsize: int
    :param outpath: location where to store the output files
    :type outpath: str
    :param outfile: name of the training data file
    :type outfile: str
    :param holdoutfile: name of the holdout data file
    :type holdoutfile: str

    :return: None
    """
    # drop UPRN
    data.drop(['UPRN'], axis=1, inplace=True)

    # re-order the columns to match the order expected in an address
    neworder = ['SubBuildingPrefix',
                'SubBuildingNumber',
                'BuildingNumber',
                'BuildingNumberSuffix',
                'BuildingName',
                'SubBuildingName',
                'OrganisationName',
                'DepartmentName',
                'StreetName',
                'TownName',
                'Locality',
                'Postcode']
    data = data[neworder]
    print(data.info())

    # training data
    if len(data.index) > trainingsize:
        #training = data.sample(n=trainingsize)
        rows = np.random.choice(data.index.values, trainingsize)
        msk = np.in1d(data.index.values, rows)
        training = data.loc[msk]
        holdout = data.loc[~msk]
    else:
        print('Only', len(data.index), 'addresses, using all for training')
        training = data
        holdout = None

    # write training data to an XML
    fh = open(outpath + outfile, mode='w')
    fh.write('<AddressCollection>')
    fh.write(''.join(training.apply(_toXML, axis=1)))
    fh.write('\n</AddressCollection>')
    fh.close()

    # write rest to a holdoutfile
    if holdout is not None:
        fh = open(outpath + holdoutfile, mode='w')
        fh.write('<AddressCollection>')
        fh.write(''.join(holdout.apply(_toXML, axis=1)))
        fh.write('\n</AddressCollection>')
        fh.close()


if __name__ == "__main__":
    data = combineAddressBaseData()
    createTrainingData(data)
