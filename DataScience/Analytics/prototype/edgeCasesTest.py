"""
ONS Address Index - Edge Case Testing
=====================================

A simple script to test parsing and matching of edge cases - 5k dataset of different types of addresses.
This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas.


Requirements
------------

:requires: ProbabilisticParser (CRF model)
:requires: pandas
:requires: numpy
:requires: matplotlib
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 1.0
:date: 17-Oct-2016
"""
import pandas as pd
import numpy as np
import recordlinkage
from ProbabilisticParser import parser
import matplotlib.pyplot as plt
import time
import re
from Analytics.data import data


def loadEdgeCaseTestingData(filename='EDGE_CASES_EC5K_NoPostcode.csv',
                            path='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                            verbose=False):
    """
    Read in the testing data.

    :param filename: name of the CSV file holding the edge case data
    :param path: location of the test data
    :param verbose: whether or not output information

    :return: pandas dataframe, which includes the edge cases data
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(path + filename)

    if verbose:
        print(df.info())

    # change column names
    df.rename(columns={'UPRN': 'uprn_edge'}, inplace=True)

    print('Found', len(df.index), 'Edge Cases...')

    return df


def loadAddressBaseData(filename='AB.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/'):
    """
    Load a compressed version of the full AddressBase file. The information being used
    has been processed from a AB Epoch 39 files provided by ONS.

    :param filename: name of the file containing modified AddressBase
    :type filename: str
    :param path: location of the AddressBase combined data file
    :type path: str

    :return: pandas dataframe of the requested information
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(path + filename, dtype={'UPRN': np.int64, 'POSTCODE_LOCATOR': str, 'ORGANISATION_NAME': str,
                                             'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                                             'BUILDING_NUMBER': str, 'THROUGHFARE': str, 'DEPENDENT_LOCALITY': str,
                                             'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str, 'PAO_START_NUMBER': str,
                                             'SAO_TEXT': str, 'SAO_START_NUMBER': str, 'ORGANISATION': str,
                                             'STREET_DESCRIPTOR': str, 'TOWN_NAME': str, 'LOCALITY': str})
    print('Found', len(df.index), 'addresses from AddressBase...')

    # combine information
    msk = df['THROUGHFARE'].isnull()
    df.loc[msk, 'THROUGHFARE'] = df.loc[msk, 'STREET_DESCRIPTOR']

    msk = df['ORGANISATION_NAME'].isnull()
    df.loc[msk, 'ORGANISATION_NAME'] = df.loc[msk, 'ORGANISATION']

    msk = df['POSTCODE'].isnull()
    df.loc[msk, 'POSTCODE'] = df.loc[msk, 'POSTCODE_LOCATOR']

    msk = df['SUB_BUILDING_NAME'].isnull()
    df.loc[msk, 'SUB_BUILDING_NAME'] = df.loc[msk, 'SAO_TEXT']

    msk = df['POST_TOWN'].isnull()
    df.loc[msk, 'POST_TOWN'] = df.loc[msk, 'TOWN_NAME']

    msk = df['LOCALITY'].isnull()
    df.loc[msk, 'LOCALITY'] = df.loc[msk, 'DEPENDENT_LOCALITY']

    # drop some that are not needed
    df.drop(['DEPENDENT_LOCALITY', 'POSTCODE_LOCATOR', 'STREET_DESCRIPTOR'], axis=1, inplace=True)

    pcodes = df['POSTCODE'].str.split(' ', expand=True)
    pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
    df = pd.concat([df, pcodes], axis=1)

    # rename some columns
    df.rename(columns={'THROUGHFARE': 'streetName',
                       'POST_TOWN': 'townName',
                       'POSTCODE': 'postcode',
                       'PAO_TEXT': 'pao_text',
                       'LOCALITY': 'locality',
                       'BUILDING_NAME': 'buildingName'}, inplace=True)

    return df


def loadAddressBaseDataFromDB():
    """
    Load AddressBase data from a database.

    :return: pandas dataframe of the requested information
    :rtype: pandas.DataFrame
    """
    df = data.queryDB('''SELECT UPRN, address, POSTCODE_LOCATOR as postcode, STREET_DESCRIPTION,
                      concat_ws('', sao_start_number, sao_start_suffix, pao_start_number, pao_start_suffix) as number,
                      pao_text, LOCALITY, TOWN_NAME FROM addresses''')
    print('Found', len(df.index), 'addresses from AddressBase...')

    # convert everything to lower case
    for tmp in df.columns:
        try:
            df[tmp] = df[tmp].str.lower()
        except:
            pass

    return df


def loadPostcodeInformation(file='/Users/saminiemi/Projects/ONS/AddressIndex/data/postcode_district_to_town.csv'):
    """

    :param file:
    :return:
    """
    df = pd.read_csv(file)
    df['town'] = df.apply(lambda x: x['town'].replace('. ', ' '), axis=1)
    df['town'] = df.apply(lambda x: x['town'].lower(), axis=1)
    df['postcode'] = df.apply(lambda x: x['postcode'].lower(), axis=1)

    return df


def getPostcode(string):
    """
    Extract a postcode from address information.

    Uses regular expression to extract the postcode:
    http://stackoverflow.com/questions/164979/uk-postcode-regex-comprehensive

    :param string: string to be parsed
    :type string: str

    :return: postcode
    :rtype: str
    """
    regx = r'(([gG][iI][rR] {0,}0[aA]{2})|((([a-pr-uwyzA-PR-UWYZ][a-hk-yA-HK-Y]?[0-9][0-9]?)|(([a-pr-uwyzA-PR-UWYZ][0-9][a-hjkstuwA-HJKSTUW])|([a-pr-uwyzA-PR-UWYZ][a-hk-yA-HK-Y][0-9][abehmnprv-yABEHMNPRV-Y]))) {0,}[0-9][abd-hjlnp-uw-zABD-HJLNP-UW-Z]{2}))'
    try:
        tmp = re.findall(regx, string)[0][0]
        tmp = tmp.lower().strip()
    except:
        tmp = None

    # above regex gives also those without space between, add if needed
    if tmp is not None:
        if ' ' not in tmp:
            inc = tmp[-3:]
            out = tmp.replace(inc, '')
            tmp = out + ' ' + inc

    return tmp


def testIfIllformattedPostcode(string):
    """

    :param string:
    :return:
    """
    try:
        tmp = \
        re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]{1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA',
                   string)[0]
        return True
    except:
        return False


def _splitRoadHouse(row, part):
    try:
        tmp = row['road'].split('house')[part]
        if part == 0:
            tmp += 'house'
    except:
        tmp = row['road']
    return tmp.strip()


def _fixBuildingNumber(row):
    if row['BuildingName'] is not None:
        tmp = row['BuildingName'].split(' ')
        if len(tmp) > 1:
            try:
                number = int(tmp[0])
                return number
            except:
                return None
        else:
            return row['BuildingName']
    else:
        return None


def parseEdgeCaseData(df):
    """
    Parses the address information from the edge case data. Examples:

    :param df: pandas dataframe containing ADDRESS column that is being parsed
    :type df: pandas.DataFrame

    :return: pandas dataframe where the parsed information has been included
    :rtype: pandas.DataFrame
    """
    # make a copy of the actual address field and run the parsing against it
    df['ADDRESS2'] = df['ADDRESS'].copy()

    # parsing gets really confused if region or county is in the line
    # for a quick hack I remove these, but regions should probably be part of the training as might help to identify
    # the correct area if no postcode
    remove = ('WEST MIDLANDS', 'WEST YORKSHIRE', 'S YORKSHIRE', 'N YORKSHIRE', 'W YORKSHIRE', 'LANCS', 'LINCS',
              'LEICS', 'HERTS', 'WARKS', 'BUCKS', 'BERKS', 'HANTS', 'WILTS', 'WORCS', 'MIDDX', 'STAFFS', 'W SUSSEX',
              'E SUSSEX', 'KENT', 'SOUTH GLAMORGAN', 'MID GLAMORGAN', 'WEST GLAMORGAN', 'ESSEX', 'SURREY', 'SUFFOLK',
              'CHESHIRE', 'CARMARTHENSHIRE', 'DERBYSHIRE', 'BERKSHIRE', 'YORKSHIRE', 'HEREFORDSHIRE', 'LINCOLNSHIRE',
              'NOTTINGHAMSHIRE', 'OXFORDSHIRE', 'BUCKINGHAMSHIRE', 'SHROPSHIRE', 'DORSET', 'DEVON', 'SOMERSET',
              'CORNWALL', 'CLEVELAND', 'NORFOLK', 'STAFFORDSHIRE', 'MIDDLESEX', 'MERSEYSIDE', 'NORTH HUMBERSIDE',
              'SOUTH HUMBERSIDE', 'ISLE OF WIGHT', '\'')

    for r in remove:
        df['ADDRESS2'] = df['ADDRESS2'].str.replace(r, '', case=False)

    # remove commas and apostrophes and insert space
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(',', ' '), axis=1)

    # remove blackslash if present and replace with space
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('\\', ' '), axis=1)

    # modify some names to help with parsing
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' STOKE ON TRENT ', ' STOKE-ON-TRENT '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' SOUTHEND ON SEA ', ' SOUTHEND-ON-SEA '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' WESTCLIFF ON SEA ', ' WESTCLIFF-ON-SEA '), axis=1)

    # get addresses and store separately as an vector
    addresses = df['ADDRESS2'].values
    print('Parsing', len(addresses), 'addresses...')

    # temp data storage
    organisation = []
    department = []
    subbuilding = []
    buildingname = []
    buildingnumber = []
    street = []
    locality = []
    town = []
    postcode = []

    # loop over addresses - quite inefficient, should avoid a loop
    for address in addresses:
        parsed = parser.tag(address.upper())
        pcode = getPostcode(address) # regular expression extraction

        # if both parsers found postcode then check that they are the same
        if parsed.get('Postcode', None) is not None and pcode is not None:
            if parsed['Postcode'] != pcode:
                # not the same, use pcode
                parsed['Postcode'] = pcode

        # if the probabilistic parser did not find postcode but regular expression did, then use that
        if parsed.get('Postcode', None) is None and pcode is not None:
            parsed['Postcode'] = pcode

        if parsed.get('Postcode', None) is not None:
            # check that there is space, if not then add
            if ' ' not in parsed['Postcode']:
                inc = parsed['Postcode'][-3:]
                out = parsed['Postcode'].replace(inc, '')
                parsed['Postcode'] = out + ' ' + inc

            # change to all capitals
            parsed['Postcode'] = parsed['Postcode'].upper()

        organisation.append(parsed.get('OrganisationName', None))
        department.append(parsed.get('DepartmentName', None))
        subbuilding.append(parsed.get('SubBuildingName', None))
        buildingname.append(parsed.get('BuildingName', None))
        buildingnumber.append(parsed.get('BuildingNumber', None))
        street.append(parsed.get('StreetName', None))
        locality.append(parsed.get('Locality', None))
        town.append(parsed.get('TownName', None))
        postcode.append(parsed.get('Postcode', None))

    # add the parsed information to the dataframe
    df['OrganisationName'] = organisation
    df['DepartmentName'] = department
    df['SubBuildingName'] = subbuilding
    df['BuildingName'] = buildingname
    df['BuildingNumber'] = buildingnumber
    df['StreetName'] = street
    df['Locality'] = locality
    df['TownName'] = town
    df['Postcode'] = postcode

    # split the postcode to in and out
    pcodes = df['Postcode'].str.split(' ', expand=True)
    pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
    df = pd.concat([df, pcodes], axis=1)

    # if BuildingNumber is empty, sometimes it's in BuildingName, try grabbing it
    msk = df['BuildingNumber'].isnull()
    df['temp'] = None
    potentials = df.loc[msk, 'BuildingName']
    df.loc[msk, 'temp'] = potentials
    df.loc[msk, 'BuildingNumber'] = df.loc[msk].apply(_fixBuildingNumber, axis=1)
    msk = df['BuildingNumber'] == df['BuildingName']
    df.loc[msk, 'BuildingName'] = None

    # # split flat or apartment number as separate for numerical comparison
    # df['flat_number'] = None
    # msk = df['flat'].str.contains('flat|apartment', na=False)
    # df.loc[msk, 'flat_number'] = df.loc[msk, 'flat']
    # df.loc[msk, 'flat_number'] = df.loc[msk].apply(lambda x: x['flat_number'].strip().replace('flat', '').replace('apartment', ''), axis=1)
    # df['flat_number'] = pd.to_numeric(df['flat_number'], errors='coerce')
    #
    # # remove those with numbers from flat column - no need to double check
    # msk = ~df['flat_number'].isnull()
    # df.loc[msk, 'flat'] = None
    # df.loc[df['flat'].isnull(), 'flat'] = 'NO'
    # df.loc[df['house'].isnull(), 'house'] = 'NO'
    # # df.loc[df['building_name'].isnull(), 'building_name'] = 'NONAME'
    # df.loc[df['house_number_suffix'].isnull(), 'house_number_suffix'] = 'NOSUFFIX'

    # some funky postcodes, remove these
    msk = df['postcode_in'] == 'Z1'
    df.loc[msk, 'postcode_in'] = None
    df.loc[msk, 'Postcode'] = None
    msk = df['postcode_out'].str.contains('[0-9][^ABCDEFGHIJKLMNOPQRSTX][Z]', na=False)
    # msk = df['postcode_out'].str.contains('Z', na=False)
    df.loc[msk, 'postcode_out'] = None
    df.loc[msk, 'Postcode'] = None
    msk = df['postcode_in'] == 'Z11'
    df.loc[msk, 'postcode_in'] = None
    df.loc[msk, 'Postcode'] = None

    # save for inspection
    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/ParsedAddresses.csv', index=False)

    # drop the temp info
    df.drop(['ADDRESS2', 'temp'], axis=1, inplace=True)

    return df


def matchDataWithPostcode(AddressBase, toMatch, houseNumberBlocking=True, limit=0.1):
    """
    Match toMatch data against the AddressBase source information.

    Uses blocking to speed up the matching. This is dangerous for postcodes that have been misspelled
    and will potentially lead to false positives.

    :param AddressBase: address based dataframe which functions as the source
    :type AddressBase: pandas.DataFrame
    :param toMatch: dataframe holding the address information that is to be matched against a source
    :type toMatch: pandas.DataFrame
    :param limit: the sum of the matching metrics need to be above this limit to count as a potential match.
                  Affects for example the false positive rate.
    :type limit: float

    :return: dataframe of matches
    :rtype: pandas.DataFrame
    """
    # create pairs
    pcl = recordlinkage.Pairs(toMatch, AddressBase)

    # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
    # block on both postcode and house number, street name can have typos and therefore is not great for blocking
    if houseNumberBlocking:
        print('Start matching those with postcode information, using postcode and house number blocking...')
        pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'], right_on=['postcode', 'PAO_START_NUMBER'])
    else:
        print('Start matching those with postcode information, using postcode and street name blocking...')
        pairs = pcl.block(left_on=['Postcode', 'StreetName'], right_on=['postcode', 'streetName'])

    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)

    # set rules for simple addresses
    if houseNumberBlocking:
        compare.string('streetName', 'StreetName', method='damerau_levenshtein', name='street_dl')
    # compare.string('PAO_START_SUFFIX', 'house_number_suffix', method='damerau_levenshtein',
    #                missing_value=0, name='pao_suffix_dl')
    compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl')
    compare.string('buildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
    compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl')
    compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl')

    # set rules for carehome type addresses
    compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl')
    compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl')
    compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl')
    # compare.string('ORGANISATION_NAME', 'OrganisationName', method='damerau_levenshtein', name='organisation2_dl')
    if ~houseNumberBlocking:
        # compare.numeric('PAO_START_NUMBER', 'BuildingNumber', threshold=0.1, missing_value=-123, name='pao_number_dl')
        compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl')

    # compare.string('SAO_START_NUMBER', 'flat_number', method='damerau_levenshtein', name='sao_number_dl')

    # execute the comparison model
    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    # compare.vectors['pao_suffix_dl'] *= 10. # helps with addresses with suffix e.g. 55A
    compare.vectors['pao_dl'] *= 5. # helps with carehomes
    compare.vectors['organisation_dl'] *= 4.
    compare.vectors['flat_dl'] *= 3.
    # compare.vectors['sao_number_dl'] *= 2.
    compare.vectors['flatw_dl'] *= 1.

    # add sum of the components to the comparison vectors dataframe
    compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)

    # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
    matches = compare.vectors.loc[compare.vectors['similarity_sum'] > limit]

    # to pick the most likely match we sort by the sum of the similarity and pick the top
    # sort matches by the sum of the vectors and then keep the first
    matches = matches.sort_values(by='similarity_sum', ascending=False)

    # reset index
    matches = matches.reset_index()

    # keep first if duplicate in the EC_Index column
    matches = matches.drop_duplicates('EC_Index', keep='first')

    # sort by EC_Index
    matches = matches.sort_values(by='EC_Index')

    print('Found ', len(matches.index), 'matches...')

    return matches


def matchDataNoPostcode(AddressBase, toMatch, limit=0.7):
    """
    Match toMatch data against the AddressBase source information.

    Uses blocking to speed up the matching.

    :param AddressBase: address based dataframe which functions as the source
    :type AddressBase: pandas.DataFrame
    :param toMatch: dataframe holding the address information that is to be matched against a source
    :type toMatch: pandas.DataFrame
    :param limit: the sum of the matching metrics need to be above this limit to count as a potential match.
                  Affects for example the false positive rate.
    :type limit: float

    :return: dataframe of matches
    :rtype: pandas.DataFrame
    """
    print('Start matching those without postcode information...')

    # create pairs
    pcl = recordlinkage.Pairs(toMatch, AddressBase)

    # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
    pairs = pcl.block(left_on=['BuildingNumber', 'StreetName'], right_on=['PAO_START_NUMBER', 'streetName'])
    # pairs = pcl.sortedneighbourhood('postcode_in', window=3, block_on='postcode_in')
    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)
    # compare.string('PAO_START_SUFFIX', 'house_number_suffix', method='damerau_levenshtein', name='pao_suffix_dl')
    compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl') # good for care homes
    compare.string('buildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
    compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl')
    compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='number_dl')
    compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl')
    compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl')
    compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl')
    compare.string('ORGANISATION_NAME', 'OrganisationName', method='damerau_levenshtein', name='organisation2_dl')
    # compare.string('SAO_START_NUMBER', 'flat_number', method='damerau_levenshtein', name='sao_number_dl')
    compare.string('townName', 'TownName', method='damerau_levenshtein', name='city_dl')
    compare.string('postcode_in', 'postcode_in', method='damerau_levenshtein', name='postcode_in_dl')
    # compare.numeric('flat_number', 'flat_number', threshold=0.1, missing_value=-123, name='flat_number_dl')
    # compare.exact('flat_number', 'flat_number', missing_value='-1234', disagree_value=-0.1, name='flat_number_dl')
    # compare.string('flat_number', 'flat_number',  method='damerau_levenshtein', name='flat_number_dl')
    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    compare.vectors['pao_dl'] *= 4.
    compare.vectors['city_dl'] *= 5.
    # compare.vectors['pao_suffix_dl'] *= 10. # helps with addresses with suffix e.g. 55A
    compare.vectors['number_dl'] *= 10.
    # compare.vectors['flat_number_dl'] *= 8.
    compare.vectors['organisation_dl'] *= 5.

    # add sum of the components to the comparison vectors dataframe
    compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)
    # normalise the comparison vectors and the sum to be between zero and unity
    # compare.vectors /= compare.vectors.max()

    # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
    matches = compare.vectors.loc[compare.vectors['similarity_sum'] > limit]

    # to pick the most likely match we sort by the sum of the similarity and pick the top
    # sort matches by the sum of the vectors and then keep the first
    matches = matches.sort_values(by='similarity_sum', ascending=False)

    # reset index
    matches = matches.reset_index()

    # keep first if duplicate in the EC_Index column
    matches = matches.drop_duplicates('EC_Index', keep='first')

    # sort by EC_Index
    matches = matches.sort_values(by='EC_Index')

    print('Found ', len(matches.index), 'matches...')

    return matches


def mergeMatchedAndAB(matches, toMatch, AddressBase):
    """
    Merge address base information to the identified matches.
    Outputs the merged information to a CSV file for later inspection.

    :param matches:
    :param toMatch:
    :param AddressBase:

    :return: merged dataframe
    :rtype: pandas.DataFrame
    """
    # merge to original information to the matched data
    toMatch = toMatch.reset_index()
    data = pd.merge(matches, toMatch, how='left', on='EC_Index')
    data = pd.merge(data, AddressBase, how='left', on='AB_Index')

    # drop unnecessary columns
    # data.drop(['EC_Index', 'AB_Index'], axis=1, inplace=True)

    return data


def checkPerformance(df, edgeCases):
    """
    Check performance - calculate for example match rate and the number of false postives.
    Show the performance for the full dataset and for each class.

    :param df:
    :param edgeCases:

    :return: None
    """
    # count the number of matches and number of edge cases
    nmatched = len(df.index)
    all = len(edgeCases.index)

    # how many were matched
    print('\nMatched', nmatched, 'entries')
    print('Total Match Fraction', round(nmatched / all * 100., 1))

    # how many were correctly matched and false positives
    msk = df['UPRN'] == df['uprn_edge']
    correct = df.loc[msk]
    fp = len(df.loc[~msk].index)

    print('Correctly Matched', len(correct.index))
    print('Correctly Matched Fraction', round(len(correct.index)/all*100., 1))

    print('False Positives', fp)
    print('False Positive Rate', round(fp/all*100., 1))

    # save false positives
    df.loc[~msk].to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_matched_false_positives.csv',
                        index=False)
    # save correctly matched
    df.loc[msk].to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_correctly_matched.csv',
                        index=False)

    # find those that were not found
    uprns = df['uprn_edge'].values
    missing_msk = ~edgeCases['uprn_edge'].isin(uprns)
    missing = edgeCases.loc[missing_msk]
    missing.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_matched_missing.csv', index=False)

    # print out results for each class separately
    mne = []
    matchf = []
    fpf = []
    for mnemonic in sorted(set(df['MNEMONIC'].values)):
        msk = (df['UPRN'] == df['uprn_edge']) & (df['MNEMONIC'] == mnemonic)
        correct = df.loc[msk]
        nmatched = len(correct.index)
        outof = len(edgeCases.loc[edgeCases['MNEMONIC'] == mnemonic].index)
        fp = len(df.loc[(df['UPRN'] != df['uprn_edge']) & (df['MNEMONIC'] == mnemonic)].index)

        print('Correctly Matched', nmatched, mnemonic)
        print('Match Fraction', round(nmatched / outof *100., 1))
        print('False Positives', fp)
        print('False Positive Rate', round(fp / outof * 100., 1))

        mne.append(mnemonic)
        matchf.append((nmatched / outof * 100.))
        fpf.append(fp / outof * 100.)

    # make a simple visualisation
    x = np.arange(len(mne))
    plt.figure(figsize=(12, 10))

    width = 0.35
    p1 = plt.bar(x, matchf, width, color='g')
    p2 = plt.bar(x + width, fpf, width, color='r')
    plt.ylabel('Fraction of the Sample')
    plt.title('Edge Case - Prototype Matching')
    plt.xticks(x + width, mne, rotation=45)
    plt.tight_layout()
    plt.savefig('/Users/saminiemi/Projects/ONS/AddressIndex/figs/EdgeCases.png')
    plt.close()


    # confusion matrix
    # recordlinkage.confusion_matrix()
    # precision
    # recall
    # f-score
    # number of false positives and fp rate


def runAll():
    """
    Run all required steps.

    :return: None
    """
    # print('\nReading in Postcode Data...')
    # postcodeinfo = loadPostcodeInformation()

    print('\nReading in Address Base Data...')
    start = time.clock()
    # ab = loadAddressBaseData()
    ab = loadAddressBaseData(filename='ABmini.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/miniAB/')
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nReading in Edge Case data...')
    start = time.clock()
    edgeCases = loadEdgeCaseTestingData()#filename='MissingPostcodesTest.csv')
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nParsing Edge Case data...')
    start = time.clock()
    parsedEdgeCases = parseEdgeCaseData(edgeCases)
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    # set index names - needed later for merging / duplicate removal
    ab.index.name = 'AB_Index'
    parsedEdgeCases.index.name = 'EC_Index'

    # split to those with full postcode and no postcode - use different matching strategies
    msk = parsedEdgeCases['Postcode'].isnull()
    withPC = parsedEdgeCases.loc[~msk]
    noPC = parsedEdgeCases.loc[msk]

    print('\nMatching Edge Cases to Address Base data...')
    start = time.clock()
    ms1 = ms2 = m1a = m1b = False
    if len(withPC.index) > 0:
        msk = withPC['BuildingNumber'].isnull()
        withPCnoHouseNumber = withPC.loc[msk]
        withPCHouseNumber = withPC.loc[~msk]
        if len(withPCHouseNumber) > 0:
            matches1a = matchDataWithPostcode(ab, withPCHouseNumber, houseNumberBlocking=True)
            m1a = True
        if len(withPCnoHouseNumber) > 0:
            matches1b = matchDataWithPostcode(ab, withPCnoHouseNumber, houseNumberBlocking=False)
            m1b = True
        ms1 = True
    if len(noPC.index) > 0:
        matches2 = matchDataNoPostcode(ab, noPC)
        ms2 = True
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nMerging back the original information...')
    start = time.clock()
    ab = ab.reset_index()
    # most horrifying code ever... should rewrite completely
    m1 = m2 = False
    if ms1:
        if m1a:
            matched1a = mergeMatchedAndAB(matches1a, parsedEdgeCases, ab)
        if m1b:
            matched1b = mergeMatchedAndAB(matches1b, parsedEdgeCases, ab)
        m1 = True
    if ms2:
        matched2 = mergeMatchedAndAB(matches2, parsedEdgeCases, ab)
        m2 = True

    if m1a & m1b:
        matched1 = matched1a.append(matched1b)
    elif m1a:
        matched1 = matched1a
    else:
        matched1 = matched1b

    if m2 & m1:
        matched = matched1.append(matched2)
    elif m1:
        matched = matched1
    else:
        matched = matched2

    matched.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_matched.csv', index=False)
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nChecking Performance...')
    checkPerformance(matched, edgeCases)


if __name__ == "__main__":
    runAll()

    """
    This version with full AB and reasonable runtime:
        NA
    On Mini:
        Matched 3470 entries
        Total Match Fraction 69.4
        Correctly Matched 3451
        Correctly Matched Fraction 69.0
        False Positives 19
        False Positive Rate 0.4
        Correctly Matched 719 CARE_HOMES
        Match Fraction 71.9
        False Positives 3
        False Positive Rate 0.3
        Correctly Matched 967 DEAD_SIMPLE
        Match Fraction 96.7
        False Positives 0
        False Positive Rate 0.0
        Correctly Matched 839 ORDER_MATTERS
        Match Fraction 83.9
        False Positives 2
        False Positive Rate 0.2
        Correctly Matched 328 PAF_MISMATCH
        Match Fraction 32.8
        False Positives 1
        False Positive Rate 0.1
        Correctly Matched 598 PARTS_MISSING
        Match Fraction 59.8
        False Positives 13
        False Positive Rate 1.3
    """