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

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: pandas
:requires: numpy
:requires: matplotlib
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 1.1
:date: 1-Nov-2016
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

    # combine information - could be done differently, but for now using some of these for blocking
    msk = df['THROUGHFARE'].isnull()
    df.loc[msk, 'THROUGHFARE'] = df.loc[msk, 'STREET_DESCRIPTOR']

    msk = df['ORGANISATION_NAME'].isnull()
    df.loc[msk, 'ORGANISATION_NAME'] = df.loc[msk, 'ORGANISATION']

    msk = df['POSTCODE'].isnull()
    df.loc[msk, 'POSTCODE'] = df.loc[msk, 'POSTCODE_LOCATOR']

    msk = df['SUB_BUILDING_NAME'].isnull()
    df.loc[msk, 'SUB_BUILDING_NAME'] = df.loc[msk, 'SAO_TEXT']
    # df.loc[msk, 'SUB_BUILDING_NAME'] = 'NULL'
    # msk = df['SAO_TEXT'].isnull()
    # df.loc[msk, 'SAO_TEXT'] = 'NULL'

    msk = df['POST_TOWN'].isnull()
    df.loc[msk, 'POST_TOWN'] = df.loc[msk, 'TOWN_NAME']

    msk = df['LOCALITY'].isnull()
    df.loc[msk, 'LOCALITY'] = df.loc[msk, 'DEPENDENT_LOCALITY']

    # drop some that are not needed
    df.drop(['DEPENDENT_LOCALITY', 'POSTCODE_LOCATOR'], axis=1, inplace=True)

    pcodes = df['POSTCODE'].str.split(' ', expand=True)
    pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
    df = pd.concat([df, pcodes], axis=1)

    # rename some columns
    df.rename(columns={'THROUGHFARE': 'StreetName',
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
    Load in a simple dataframe joining postcodes to post towns.

    :param file: name of the CSV containing the mapping

    :return:
    """
    df = pd.read_csv(file)
    df['town'] = df.apply(lambda x: x['town'].replace('. ', ' '), axis=1)
    df['town'] = df.apply(lambda x: x['town'].lower(), axis=1)
    df['postcode'] = df.apply(lambda x: x['postcode'].lower(), axis=1)

    return df


def getPostcode(string):
    """
    Extract a postcode from address string. Uses rather loose regular expression, so
    may get some strings that are not completely valid postcodes.

    The regular expression is taken from:
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
                return tmp[0]
            except:
                return None
        else:
            return None
    else:
        return None


def _normalizeData(df, expandSynonyms=True):
    """
    Normalize the address information. Removes white spaces, commas, and backslashes.
    Can be used to expand common synonyms such as RD or BERKS. Finally parses counties
    as the an early version of the probabilistic parser was not trained to parser counties.

    :param df: pandas dataframe containing ADDRESS column that is being copied to ADDRESS2 and modified
    :type df: pandas.DataFrame
    :param expandSynonyms: whether to expand common synonyms or not
    :type expandSynonyms: bool

    :return: pandas dataframe where the normalised address information is in column ADDRESS2
    :rtype: pandas.DataFrame
    """
    # make a copy of the actual address field and run the parsing against it
    df['ADDRESS2'] = df['ADDRESS'].copy()

    # remove white spaces if present
    df['ADDRESS2'] = df['ADDRESS2'].str.strip()

    # remove commas and apostrophes and insert space
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(',', ' '), axis=1)

    # remove backslash if present and replace with space
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('\\', ' '), axis=1)

    # synonyms to expand - format is [(from, to), ]
    synonyms = [(' AVEN ', ' AVENUE '),
                (' AVE ', ' AVENUE '),
                (' AV ', ' AVENUE '),
                (' LN ', ' LANE '),
                (' APPTS ', ' APARTMENT '),
                (' APPT ', ' APARTMENT '),
                (' APTS ', ' APARTMENT '),
                (' APT ', ' APARTMENT '),
                (' BLK ', ' BLOCK '),
                (' BVLD ', ' BOULEVARD '),
                (' DR ', ' DRIVE '),
                (' RD ', ' ROAD '),
                (' PK ', ' PARK '),
                (' STR ', ' STREET '),
                (' NOS ', ' NUMBER '),
                (' NO ', ' NUMBER '),
                (' HSE ', ' HOUSE '),
                (' BERKS(?:\s|\Z)', ' BERKSHIRE '),
                (' WARKS(?:\s|\Z)', ' WARWICKSHIRE '),
                (' BUCKS(?:\s|\Z)', ' BUCKINGHAMSHIRE '),
                (' HANTS(?:\s|\Z)', ' HAMPSHIRE '),
                (' LEICS(?:\s|\Z)', ' LEICESTERSHIRE '),
                (' LINCS(?:\s|\Z)', ' LINCOLNSHIRE '),
                (' LANCS(?:\s|\Z)', ' LANCASHIRE '),
                (' MIDDX(?:\s|\Z)', ' MIDDLESEX '),
                (' STAFFS(?:\s|\Z)', ' STAFFORDSHIRE '),
                (' WORCS(?:\s|\Z)', ' WORCESTERSHIRE '),
                (' WILTS(?:\s|\Z)', ' WILTSHIRE '),
                (' HERTS(?:\s|\Z)', ' HERTFORDSHIRE '),
                (' CAMBS(?:\s|\Z)', ' CAMBRIDGESHIRE '),
                (' OXON(?:\s|\Z)', ' OXFORDSHIRE '),
                (' HFDS(?:\s|\Z)', ' HERTFORDSHIRE '),
                (' BEDS(?:\s|\Z)', ' BEDFORDSHIRE '),
                (' STOKE ON TRENT ', ' STOKE-ON-TRENT '),
                (' SOUTHEND ON SEA ', ' SOUTHEND-ON-SEA '),
                (' WESTCLIFF ON SEA ', ' WESTCLIFF-ON-SEA ')]

    # expand common synonyms to help with parsing
    if expandSynonyms:
        print('Expanding synonyms as a part of normalisation...')
        for fro, to in synonyms:
            df['ADDRESS2'] = df['ADDRESS2'].str.replace(fro, to)

    # parsing gets really confused if region or county is in the line
    counties = ('WEST MIDLANDS', 'WEST YORKSHIRE', 'S YORKSHIRE', 'N YORKSHIRE', 'W YORKSHIRE', 'W SUSSEX',
                'E SUSSEX', 'KENT', 'SOUTH GLAMORGAN', 'MID GLAMORGAN', 'WEST GLAMORGAN', 'ESSEX', 'SURREY', 'SUFFOLK',
                'CHESHIRE', 'CARMARTHENSHIRE', 'DERBYSHIRE', 'BERKSHIRE', 'YORKSHIRE', 'HEREFORDSHIRE', 'LINCOLNSHIRE',
                'NOTTINGHAMSHIRE', 'OXFORDSHIRE', 'BUCKINGHAMSHIRE', 'SHROPSHIRE', 'DORSET', 'DEVON', 'SOMERSET',
                'CORNWALL', 'CLEVELAND', 'NORFOLK', 'STAFFORDSHIRE', 'MIDDLESEX', 'MERSEYSIDE', 'NORTH HUMBERSIDE',
                'SOUTH HUMBERSIDE', 'ISLE OF WIGHT', 'CUMBRIA', 'FLINTSHIRE', 'GLOUCESTERSHIRE', 'WILTSHIRE',
                'DENBIGHSHIRE', 'TYNE AND WEAR', 'NORTHUMBERLAND', 'NORTHAMPTONSHIRE', 'WARWICKSHIRE', 'HAMPSHIRE',
                'GWENT', 'NORFOLK', 'CHESHIRE', 'POWYS', 'LEICESTERSHIRE', 'NORTHAMPTONSHIRE', 'NORTHANTS',
                'WORCESTERSHIRE', 'HERTFORDSHIRE', 'CAMBRIDGESHIRE', 'BEDFORDSHIRE', 'LANCASHIRE')

    # remove county from address but add a column for it
    df['County'] = None
    for county in counties:
        msk = df['ADDRESS2'].str.contains(county, na=False)
        df.loc[msk, 'County'] = county
        df['ADDRESS2'] = df['ADDRESS2'].str.replace(county, '', case=False)

    return df


def parseInputData(df, expandSynonyms=True):
    """
    Parses the address information from the input data.

    :param df: pandas dataframe containing ADDRESS column that is being parsed
    :type df: pandas.DataFrame
    :param expandSynonyms: whether to expand common synonyms or not
    :type expandSynonyms: bool

    :return: pandas dataframe where the parsed information has been included
    :rtype: pandas.DataFrame
    """
    # normalise data so that the parser has the best possible chance of getting things right
    df = _normalizeData(df, expandSynonyms=expandSynonyms)

    # get addresses and store separately as an vector
    addresses = df['ADDRESS2'].values
    print('Parsing', len(addresses), 'addresses...')

    # temp data storage lists
    organisation = []
    department = []
    subbuilding = []
    buildingname = []
    buildingnumber = []
    buildingsuffix = []
    street = []
    locality = []
    town = []
    postcode = []

    # loop over addresses - quite inefficient, should avoid a loop
    for address in addresses:
        parsed = parser.tag(address.upper())    # probabilistic parser
        pcode = getPostcode(address)            # regular expression extraction

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

        # if Hackney etc. in StreetName then remove and move to locality
        # todo: probabilistic parser should see more cases with london localities, parsed incorrectly at the mo
        if parsed.get('StreetName', None) is not None:
            locs = ['HACKNEY', 'ISLINGTON', 'STRATFORD', 'EAST HAM', 'WOOD GREEN', 'FINCLEY', 'HORNSEY', 'HENDON',
                    'TOTTENHAM', 'BLACKHEATH', 'BAYSWATER', 'CHISWICK', 'COLINDALE', 'LEWISHAM', 'FOREST HILL',
                    'NORBURY', 'MANOR PARK', 'PLAISTOW', 'ABBEY WOOD', 'SOUTH NORWOOD', 'CHARLTON', 'MOTTINGHAM',
                    'NEW ELTHAM', 'BATTERSEA', 'PUTNEY', 'TOOTING', 'RAYNES PARK', 'MORTLAKE', 'WEST KENSINGTON',
                    'ACTON', 'HAMMERSMITH', 'HANWELL', 'NEW SOUTHGATE', 'GREEN LANES']
            for loc in locs:
                if loc in parsed['StreetName']:
                    parsed['Locality'] = loc
                    parsed['StreetName'] = parsed['StreetName'].replace(loc, '').strip()

        # if BuildingName is e.g. 55A then should get the number and suffix separately
        if parsed.get('BuildingName', None) is not None:
            parsed['BuildingSuffix'] = ''.join([x for x in parsed['BuildingName'] if not x.isdigit()])
            # accept suffixes that are only maximum two chars
            if len(parsed['BuildingSuffix']) > 2:
                parsed['BuildingSuffix'] = None

        # store the parsed information to separate lists
        organisation.append(parsed.get('OrganisationName', None))
        department.append(parsed.get('DepartmentName', None))
        subbuilding.append(parsed.get('SubBuildingName', None))
        buildingname.append(parsed.get('BuildingName', None))
        buildingnumber.append(parsed.get('BuildingNumber', None))
        street.append(parsed.get('StreetName', None))
        locality.append(parsed.get('Locality', None))
        town.append(parsed.get('TownName', None))
        postcode.append(parsed.get('Postcode', None))
        buildingsuffix.append(parsed.get('BuildingSuffix', None))

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
    df['BuildingSuffix'] = buildingsuffix

    # if valid postcode information found then split between in and outcode
    if df['Postcode'].count() > 0:
        pcodes = df['Postcode'].str.split(' ', expand=True)
        pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
        df = pd.concat([df, pcodes], axis=1)
    else:
        df['postcode_in'] = None
        df['postcode_out'] = None

    # if BuildingNumber is empty, sometimes the info is in BuildingName, try grabbing it
    msk = df['BuildingNumber'].isnull()
    df.loc[msk, 'BuildingNumber'] = df.loc[msk].apply(_fixBuildingNumber, axis=1)
    msk = df['BuildingNumber'] == df['BuildingName']
    df.loc[msk, 'BuildingName'] = None

    # # split flat or apartment number as separate for numerical comparison - compare e.g. SAO number
    df['FlatNumber'] = None
    msk = df['BuildingName'].str.contains('flat|apartment', na=False)
    df.loc[msk, 'FlatNumber'] = df.loc[msk, 'BuildingName']
    df.loc[msk, 'FlatNumber'] = df.loc[msk].apply(lambda x:
                                                  x['FlatNumber'].strip().replace('FLAT', '').replace('APARTMENT', ''),
                                                  axis=1)
    df['FlatNumber'] = pd.to_numeric(df['FlatNumber'], errors='coerce')

    # some funky postcodes, remove these
    msk = df['postcode_in'] == 'Z1'
    df.loc[msk, 'postcode_in'] = None
    df.loc[msk, 'Postcode'] = None
    msk = df['postcode_out'].str.contains('[0-9][^ABCDEFGHIJKLMNOPQRSTX][Z]', na=False)
    df.loc[msk, 'postcode_out'] = None
    df.loc[msk, 'Postcode'] = None
    msk = df['postcode_in'] == 'Z11'
    df.loc[msk, 'postcode_in'] = None
    df.loc[msk, 'Postcode'] = None

    # save for inspection
    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/ParsedAddresses.csv', index=False)

    # drop the temp info
    df.drop(['ADDRESS2', ], axis=1, inplace=True)

    return df


def matchDataWithPostcode(AddressBase, toMatch, houseNumberBlocking=True, limit=0.1):
    """
    Link toMatch data to the AddressBase source information.

    Uses blocking to speed up the matching. This is dangerous for postcodes that have been misspelled
    and will potentially lead to false positives.

    .. note: the aggressive blocking does not work when both BuildingNumber and BuildingName is missing.
             This is somewhat common for example for care homes. One should really separate these into
             different category and do blocking only on postcode.

    :param AddressBase: AddressBase dataframe which functions as the source
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
    # todo: include an option when neither BuildingNumber nor BuildingName is present (e.g. carehomes)
    if houseNumberBlocking:
        print('Start matching those with postcode information, using postcode and house number blocking...')
        # pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'], right_on=['postcode', 'PAO_START_NUMBER'])
        # better to block on building_number rather than PAO_START_NUMBER as the latter is same for 8 and 8A
        pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'], right_on=['postcode', 'BUILDING_NUMBER'])
    else:
        # print('Start matching those with postcode information, using postcode and street name blocking...')
        # pairs = pcl.block(left_on=['Postcode', 'StreetName'], right_on=['postcode', 'StreetName'])
        print('Start matching those with postcode information, using postcode and building number blocking...')
        pairs = pcl.block(left_on=['Postcode', 'BuildingName'], right_on=['postcode', 'buildingName'])

    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)

    # set rules for standard residential addresses
    compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl')
    compare.string('buildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
    compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl')
    compare.string('StreetName', 'StreetName', method='damerau_levenshtein', name='street_dl')
    compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl')
    compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl')

    # the following is good for flats and apartments than have been numbered
    compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl')
    # compare.string('SAO_START_NUMBER', 'flat_number', method='damerau_levenshtein', name='sao_number_dl')

    # set rules for organisations such as care homes and similar type addresses
    compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl')
    compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl')
    # compare.string('ORGANISATION_NAME', 'OrganisationName', method='damerau_levenshtein', name='organisation2_dl')

    # execute the comparison model
    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    compare.vectors['pao_dl'] *= 5.
    compare.vectors['organisation_dl'] *= 4.
    compare.vectors['flat_dl'] *= 3.
    compare.vectors['building_name_dl'] *= 3.

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
    Link toMatch data to the AddressBase source information.
    Uses blocking to speed up the matching.

    :param AddressBase: AddressBase dataframe which functions as the source
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
    # pairs = pcl.sortedneighbourhood('StreetName', window=3,
    #                                 block_left_on='BuildingNumber', block_right_on='BUILDING_NUMBER')
    pairs = pcl.block(left_on=['BuildingNumber', 'StreetName'], right_on=['BUILDING_NUMBER', 'StreetName'])
    # while town name blocking allows a wider search space it also takes a lot longer on a laptop...
    # pairs = pcl.block(left_on=['BuildingNumber', 'TownName'], right_on=['BUILDING_NUMBER', 'townName'])
    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)

    # # compare.string('PAO_START_SUFFIX', 'house_number_suffix', method='damerau_levenshtein', name='pao_suffix_dl')
    # compare.string('STREET_DESCRIPTOR', 'StreetName', method='damerau_levenshtein', name='street_desc_dl')
    # # compare.string('SAO_START_NUMBER', 'flat_number', method='damerau_levenshtein', name='sao_number_dl')
    # # compare.numeric('flat_number', 'flat_number', threshold=0.1, missing_value=-123, name='flat_number_dl')
    # # compare.exact('flat_number', 'flat_number', missing_value='-1234', disagree_value=-0.1, name='flat_number_dl')

    compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl')
    compare.string('buildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
    compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl')
    compare.string('StreetName', 'StreetName', method='damerau_levenshtein', name='street_dl')
    compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl')
    compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl')

    # for some there might be a part of the postcode, this can be useful
    compare.string('postcode_in', 'postcode_in', method='damerau_levenshtein', name='postcode_in_dl')

    # the following is good for flats and apartments than have been numbered
    compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl')

    # set rules for organisations such as care homes and similar type addresses
    compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl')
    compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl')
    # compare.string('ORGANISATION_NAME', 'OrganisationName', method='damerau_levenshtein', name='organisation2_dl')

    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    # compare.vectors['building_name_dl'] *= 4.
    # compare.vectors['flatw_dl'] *= 8.
    # compare.vectors['pao_number_dl'] *= 10.
    # # compare.vectors['flat_number_dl'] *= 8.
    # compare.vectors['organisation_dl'] *= 5.
    compare.vectors['pao_dl'] *= 5.
    compare.vectors['town_dl'] *= 5.
    compare.vectors['organisation_dl'] *= 4.
    compare.vectors['flat_dl'] *= 3.
    compare.vectors['building_name_dl'] *= 3.
    # upweight locality

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


def mergeMatchedAndAB(matches, toMatch, AddressBase, dropColumns=False):
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
    if dropColumns:
        data.drop(['EC_Index', 'AB_Index'], axis=1, inplace=True)

    return data


def checkPerformance(df, edgeCases, prefix='EdgeCase'):
    """
    Check performance - calculate for example match rate and the number of false positives.
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
    df.loc[~msk].to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/' + prefix + '_matched_false_positives.csv',
                        index=False)
    # save correctly matched
    df.loc[msk].to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/' + prefix + '_correctly_matched.csv',
                       index=False)

    # find those that were not found
    uprns = df['uprn_edge'].values
    missing_msk = ~edgeCases['uprn_edge'].isin(uprns)
    missing = edgeCases.loc[missing_msk]
    missing.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/' + prefix + '_matched_missing.csv', index=False)

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
    plt.savefig('/Users/saminiemi/Projects/ONS/AddressIndex/figs/' + prefix + '.png')
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
    ab = loadAddressBaseData()
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nReading in Edge Case data...')
    start = time.clock()
    # edgeCases = loadEdgeCaseTestingData()
    edgeCases = loadEdgeCaseTestingData(filename='DeadSimpleNoPostcode.csv')
    # edgeCases = loadEdgeCaseTestingData(filename='DeadSimpleTest.csv')
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nParsing Edge Case data...')
    start = time.clock()
    parsedEdgeCases = parseInputData(edgeCases)
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

    # most horrifying code ever... todo: complete rewrite required
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
    elif m1b:
        matched1 = matched1b

    if m2 & m1:
        matched = matched1.append(matched2)
    elif m1:
        matched = matched1
    elif m2:
        matched = matched2

    matched.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_matched.csv', index=False)
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nChecking Performance...')
    checkPerformance(matched, edgeCases)


if __name__ == "__main__":
    runAll()

    """
    This version with full AB and reasonable runtime (aggressive blocking):
        Correctly Matched 994 DEAD_SIMPLE
        Match Fraction 99.4
        False Positives 0
        False Positive Rate 0.0
        Correctly Matched 900 DEAD_SIMPLE_NO_PC
        Match Fraction 90.0
        False Positives 20
        False Positive Rate 2.0
    On Mini version of AB:
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