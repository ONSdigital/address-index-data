"""
ONS Address Index - Edge Case Testing
=====================================

A simple script to test parsing and matching of edge cases - 5k dataset of different types of addresses.
This is a prototype code aimed for experimentation and testing not for production.


Requirements
------------

:requires: pandas
:requires: numpy
:requires: matplotlib
:requires: libpostal (https://github.com/openvenues/libpostal)
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.8
:date: 6-Oct-2016
"""
import pandas as pd
import numpy as np
import recordlinkage
from postal.parser import parse_address
import matplotlib.pyplot as plt
import time
import re
from AddressIndex.Analytics import data


def loadEdgeCaseTestingData(filename='/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K.csv',
# def loadEdgeCaseTestingData(filename='/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K_parsing_test.csv',
                            verbose=False):
    """
    Read in the edge case testing data.

    :param filename: name of the CSV file holding the edge case data
    :param verbose: whether or not output information

    :return: pandas dataframe, which includes the edge cases data
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(filename)

    if verbose:
        print(df.info())

    # change column names
    df.rename(columns={'UPRN': 'uprn_edge'}, inplace=True)

    nec = len(df.index)
    print('Found', nec, 'Edge Cases...')

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


def getIllformattedPostcodeString(string):
    """
    Extract a postcode from address information without a space between in the in and outcode.

    :param string: text string
    :type string: str

    :return: reconstructured postcode
    :rtype: str
    """
    tmp = re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]{1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA', string)[0]
    inc = tmp[-3:]
    out = tmp.replace(inc, '')
    constructedPostcode = (out + ' ' + inc).lower()

    return constructedPostcode


def getIllformattedPostcode(row):
    """
    Extract a postcode from address information without a space between in the in and outcode.

    :param row: pandas dataframe row
    :type row: pandas.draframe.row

    :return: reconstructured postcode
    :rtype: str
    """
    tmp = re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]{1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA', row['ADDRESS2'])[0]
    inc = tmp[-3:]
    out = tmp.replace(inc, '')
    constructedPostcode = (out + ' ' + inc).lower()

    return constructedPostcode


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


def loadAddressBaseData():
    """
    Load AddressBase data from a database.

    :return: pandas dataframe of the requested information
    :rtype: pandas.DataFrame
    """
    df = data.queryDB('''SELECT UPRN, address, POSTCODE_LOCATOR as postcode, STREET_DESCRIPTION,
                      concat_ws('', sao_start_number, sao_start_suffix, pao_start_number, pao_start_suffix) as number,
                      pao_text, LOCALITY, TOWN_NAME FROM addresses''')
    print('\nFound', len(df.index), 'addresses from AddressBase...')

    # convert everything to lower case
    for tmp in df.columns:
        try:
            df[tmp] = df[tmp].str.lower()
        except:
            pass

    return df


def _getPostIncode(row):
    return row['postcode'].split(' ')[0]


def _getPostOutcode(row):
    return row['postcode'].split(' ')[1]


def _splitRoadHouse(row, part):
    try:
        tmp = row['road'].split('house')[part]
        if part == 0:
            tmp += 'house'
    except:
        tmp = row['road']
    return tmp.strip()


def loadMiniAddressBaseData():
    """
    Load a subset of AddressBase data provided by ONS on the 3rd of October to enable protyping.

    :return: pandas dataframe of the requested information
    :rtype: pandas.DataFrame
    """
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/miniAB/'
    df = pd.read_csv(path + 'combined.csv')

    # convert everything to lower case
    for tmp in df.columns:
        try:
            df[tmp] = df[tmp].str.lower()
        except:
            pass

    # if SAO_TEXT is None and a value exists in SUB_BUILDING_NAME then use this
    msk = df['SAO_TEXT'].isnull()
    df.loc[msk, 'SAO_TEXT'] = df.loc[msk, 'SUB_BUILDING_NAME'].copy()

    # if SAO_TEXT or ORGANISATION is none, force it to NO
    msk = df['SAO_TEXT'].isnull()
    df.loc[msk, 'SAO_TEXT'] = 'NO'
    df.loc[df['ORGANISATION'].isnull(), 'ORGANISATION'] = 'NO'

    # split flat or apartment number as separate for numerical comparison
    df['flat_number'] = None
    msk = df['SAO_TEXT'].str.contains('flat|apartment', na=False)
    df.loc[msk, 'flat_number'] = df.loc[msk, 'SAO_TEXT']
    df['flat_number'] = df.loc[msk].apply(lambda x: x['flat_number'].strip().replace('flat', '').replace('apartment', ''), axis=1)
    df['flat_number'] = pd.to_numeric(df['flat_number'], errors='coerce')

    # change column names
    df.rename(columns={'POSTCODE_LOCATOR': 'postcode', 'STREET_DESCRIPTOR': 'street_descriptor',
                       'TOWN_NAME': 'town_name', 'BUILDING_NUMBER': 'building_number', 'PAO_TEXT': 'pao_text',
                       'SAO_TEXT': 'sao_text', 'BUILDING_NAME': 'building_name', 'LOCALITY': 'locality'}, inplace=True)

    # split the postcode to in and out
    df['postcode_in'] = df.apply(_getPostIncode, axis=1)
    df['postcode_out'] = df.apply(_getPostOutcode, axis=1)

    return df


def parseEdgeCaseData(df, postcodeinfo):
    """
    Parses the address information from the edge case data. Examples:

    Difficult and incorrectly parsed:
    [('exbury', 'road'), ('place', 'house'), ('12-13', 'house_number'), ('exbury place', 'road'), ('st peters', 'suburb'), ('worcester', 'city'), ('wr5 3tp', 'postcode')]

    Easy:
    ('mount pleasant care home', 'house'), ('18', 'house_number'), ('rosemundy', 'road'), ('st agnes', 'city'), ('tr5 0ud', 'postcode')]

    No city (london borough of hillingdon):
    [('life opportunities trust', 'house'), ('13', 'house_number'), ('devon way', 'road'), ('hillingdon', 'suburb'), ('ub10 0js', 'postcode')]

    Difficult
    [('15', 'house_number'), ('dillstone court', 'road'), ('liverpool john', 'city'), ('moores university cathedral', 'house'), ('gate', 'road'), ('liverpool', 'city'), ('l1 7bt', 'postcode')]

    :param df: pandas dataframe containing ADDRESS column that is being parsed
    :type df: pandas.DataFrame

    :return: pandas dataframe where the parsed information has been inclsuded
    :rtype: pandas.DataFrame
    """
    # make a copy of the actual address field and run the parsing against it
    df['ADDRESS2'] = df['ADDRESS'].copy()

    # parsing gets really confused if region or county is in the line
    # for a quick hack I remove these, but regions should probably be part of the training as might help to identify
    # the correct area if no postcode
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('WEST MIDLANDS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('WEST YORKSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('S YORKSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('N YORKSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('W YORKSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('LANCS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('LINCS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('LEICS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('HERTS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('WARKS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('BUCKS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('BERKS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('HANTS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('WILTS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('WORCS', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('MIDDX', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('W SUSSEX', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('E SUSSEX', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('KENT', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SOUTH GLAMORGAN', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('MID GLAMORGAN', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('WEST GLAMORGAN', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('ESSEX', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SURREY', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SUFFOLK', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('CHESHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('DERBYSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('BERKSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('YORKSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('HEREFORDSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('LINCOLNSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('NOTTINGHAMSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('OXFORDSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('BUCKINGHAMSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SHROPSHIRE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('DORSET', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('DEVON', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SOMERSET', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('CORNWALL', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('ISLE OF WIGHT', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('CLEVELAND', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('NORFOLK', ''), axis=1)

    # remove postal counties
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('NORTH HUMBERSIDE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SOUTH HUMBERSIDE', ''), axis=1)

    # remove full stop if followed by a space
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('. ', ' '), axis=1)
    # remove commas and apostrophes
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('\' ', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('\'', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(',', ' '), axis=1)
    # remove blackslash if present
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('\\', ' '), axis=1)
    # df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('&', 'AND'), axis=1)

    # expand common synonyms
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' AVEN ', ' avenue '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' AVE ', ' avenue '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' AV ', ' avenue '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' LN ', ' lane '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' APPTS ', ' apartment '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' APPT ', ' apartment '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' APTS ', ' apartment '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' APT ', ' apartment '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' BLK ', ' block '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' BVLD ', ' boulevard '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' DR ', ' drive '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' RD ', ' road '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' PK ', ' park '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' STR ', ' street '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' NOS ', ' number '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' NO ', ' number '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' HSE ', ' house '), axis=1)

    # modify some names
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' STOKE ON TRENT ', ' STOKE-ON-TRENT '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' SOUTHEND ON SEA ', ' SOUTHEND-ON-SEA '), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' WESTCLIFF ON SEA ', ' WESTCLIFF-ON-SEA '), axis=1)

    # get addresses and store separately as an vector
    addresses = df['ADDRESS2'].values
    print('Parsing', len(addresses), 'addresses...')

    # temp data storage
    postcodes = []
    house_number = []
    house = []
    road = []
    city = []
    building_name = []
    flats = []
    locality = []

    # loop over addresses - todo: quite in efficient, should avoid a loop
    for address in addresses:
        parsed = parse_address(address) # probabilistic
        pcode = getPostcode(address) # regular expression extraction

        store = {}

        for tmp in parsed:

            if tmp[1] == 'city':
                if store.get('city', None) is not None:
                    # city already identified
                    # print(tmp[0], store['city'])
                    store['city'] = tmp[0]
                else:
                    store['city'] = tmp[0]

            if tmp[1] == 'house_number':
                store['house_number'] = tmp[0]

            if tmp[1] == 'house':
                store['house'] = tmp[0]

            if tmp[1] == 'road':
                store['road'] = tmp[0]

            if tmp[1] == 'suburb':
                store['locality'] = tmp[0]

            if tmp[1] == 'postcode':
                if tmp[0] == pcode:
                    store['postcode'] = pcode
                else:
                    if pcode is None:
                            inc = tmp[0][-3:]
                            out = tmp[0].replace(inc, '')
                            constructedPostcode = out + ' ' + inc
                            store['postcode'] = constructedPostcode
                    else:
                        store['postcode'] = pcode

        # if the probabilistic parser did not find postcode but regular expression did, then use that
        if store.get('postcode', None) is None and pcode is not None:
            store['postcode'] = pcode

        # # test if the incode actually matches the city
        # if store.get('postcode', None) is not None:
        #     tmp = store['postcode'].split(' ')
        #     msk = postcodeinfo['postcode'] == tmp[0]
        #
        #     if len(postcodeinfo.loc[msk].index) == 0:
        #         print('incorrect postcode found', store['postcode']) #if city exists then pick the first one
        #
        #         if store.get('city', None) is not None:
        #             msk3 = postcodeinfo['town'].str.contains(store['city'])
        #             if msk3.sum() == 0:
        #                 print('Cannot find anything with the city', store['city'], store['postcode'], address)
        #             else:
        #                 pass
        #                 #store['postcode'] = postcodeinfo.loc[msk3, 'postcode'].values[0] + ' ' + store['postcode'][-3:]
        #                 #print('Using city to infer postcode', address, store['postcode'])
        #     else:
        #         # found the postcode, need to test if it matches the city
        #         potentialTowns = postcodeinfo.loc[msk]
        #
        #         if store.get('city', None) is not None:
        #             #test if the city is a potential match for the postcode
        #             msk2 = potentialTowns['town'].str.contains(store['city'])
        #
        #             if len(potentialTowns.loc[msk2].index) == 0:
        #                 print('City and Postcode do not match, trying to resolve...', address)
        #                 # city might contain more than the actual city
        #                 parts = store['city'].split(' ')
        #                 if len(parts) > 1:
        #                     print('City contained multiple parts', store['city'])
        #                     for part in parts:
        #                         msk2 = potentialTowns['town'].str.contains(part)
        #                         if len(potentialTowns.loc[msk2].index) > 0:
        #                             #match change city to part, discard the other part
        #                             print('changing city from', store['city'], 'to', part, address)
        #                             store['city'] = part
        #                             # todo: add the rest to store['locality']
        #                             break
        #                 else:
        #                     # option one, typo in the postcode, search for other postcodes
        #                     msk = postcodeinfo['postcode'].str.contains(tmp[0][:2])
        #                     possiblities = postcodeinfo.loc[msk]
        #
        #                     # if city in the list of possible towns then change the postcode, otherwise change city
        #                     msk2 = possiblities['town'] == store['city'].strip().lower()
        #
        #                     if len(possiblities.loc[msk2, 'postcode']) == 0:
        #                         print('Incorrect city, using postcode to infer city...')
        #                         old = store['city']
        #                         store['city'] = possiblities['town'].values[0]
        #
        #                         if store.get('road', None) is not None:
        #                             print('swapping city and road')
        #                             store['road'] = old
        #                         print(store)
        #                     else:
        #                         incode = possiblities.loc[msk2, 'postcode'].values[0]
        #                         store['postcode'] = incode + ' ' + tmp[1]
        #                         print('Likely typo in the postcode', store['postcode'], address)
        #
        #         else:
        #             print(parsed)
        #             print('Adding city', potentialTowns['town'].values[0], 'to', address)
        #             store['city'] = potentialTowns['town'].values[0]
        #
        #             # sometimes the city is stored in a wrong field
        #             for key, value in store.items():
        #                 if 'city' in key:
        #                     pass
        #                 else:
        #                     if store['city'] in value:
        #                         store[key] = value.replace(store['city'], '')

        # sometimes house number ends up in the house field - switch these
        if store.get('house_number', None) is None and store.get('house', None) is not None:
            store['house_number'] = store['house']
            store['house'] = None

        # sometimes flats end up on the house_number column
        if store.get('house', None) is None and store.get('house_number', None) is not None:
            if 'flat' in store['house_number']:
                # sometimes both the house number and flat is combined
                tmp = store.get('house_number', None).lower().strip().split()
                if tmp[1] == 'flat' and len(tmp) == 3:
                    store['house_number'] = tmp[0]
                    store['flat'] = tmp[1] + ' ' + tmp[2]
                elif tmp[0] == 'flat' and len(tmp) == 3:
                    store['house_number'] = tmp[2]
                    store['flat'] = tmp[0] + ' ' + tmp[1]

            # sometimes care home names end up in house_number and house name is empty
            elif len(store['house_number']) > 8 or 'house' in store['house_number']:
                if 'flat' in store['house_number']:
                    tmp = house['house_number'].strip().split()
                    if 'flat' in tmp[0]:
                        house['flat'] = tmp[0] + ' ' + tmp[1]
                        house['house'] = house['house_number'].strip().replace(tmp[0], '').replace(tmp[1], '').strip()
                else:
                    store['house'] = store['house_number']
                    store['house_number'] = None

        # house number needs to be just a number if say 52a then should match with BUILDING_NAME
        try:
            store['house_number'] = int(store['house_number'])
        except:
            store['building_name'] = store.get('house_number', None)
            try:
                store['house_number'] = int(store['house_number'][:-1])
            except:
                store['house_number'] = None

        # if house number None, try to get it from the front of the string
        if store.get('house_number', None) is None:
            tmp = address.strip().split()
            try:
                store['house_number'] = int(tmp[0])
            except:
                store['house_number'] = None

        if store.get('house', None) is not None and store.get('flat', None) is None:
            if 'flat' in store.get('house', None):
                store['flat'] = store['house']
                store['house'] = None

        # if the string starts with FLAT or APARTMENT then capture that
        if address.lower().strip().startswith('flat'):
            tmp = address.lower().strip().split()
            store['flat'] = tmp[0] + ' ' + tmp[1]

        if address.lower().strip().startswith('apartment'):
            tmp = address.lower().strip().split()
            store['flat'] = tmp[0] + ' ' + tmp[1]

        # if flat contains incorrectly formatted postcode, then remove
        if testIfIllformattedPostcode(store.get('flat', None)):
            store['flat'] = None

        city.append(store.get('city', None))
        house_number.append(store.get('house_number', None))
        house.append(store.get('house', None))
        road.append(store.get('road', None))
        postcodes.append(store.get('postcode', None))
        building_name.append(store.get('building_name', None))
        flats.append(store.get('flat', None))
        locality.append(store.get('locality', None))

    # add the parsed information to the dataframe
    df['postcode'] = postcodes
    df['house_number'] = house_number
    df['house'] = house
    df['road'] = road
    df['locality'] = locality
    df['city'] = city
    df['building_name'] = building_name
    df['flat'] = flats

    # move flat from house or building_name to flat column
    msk = df['house'].str.contains('flat|apartment', na=False)
    msk2 = df['flat'].isnull()
    df.loc[msk & msk2, 'flat'] = df.loc[msk & msk2, 'house']
    df.loc[msk, 'house'] = None
    msk = df['building_name'].str.contains('flat|apartment', na=False)
    msk2 = df['flat'].isnull()
    df.loc[msk & msk2, 'flat'] = df.loc[msk & msk2, 'building_name']
    df.loc[msk, 'building_name'] = None

    # sometimes building name has ilformatted postcode, remove these
    df['tmp'] = df.apply(lambda x: x['postcode'].strip().replace(' ', ''), axis=1)
    msk = df['building_name'] == df['tmp']
    df.loc[msk, 'building_name'] = None
    df.drop('tmp', axis=1, inplace=True)

    # split the postcode to in and out - poor solution, works only if all postcodes are present
    df['postcode_in'] = df.apply(_getPostIncode, axis=1)
    df['postcode_out'] = df.apply(_getPostOutcode, axis=1)

    # split flat or apartment number as separate for numerical comparison
    df['flat_number'] = None
    msk = df['flat'].str.contains('flat|apartment', na=False)
    df.loc[msk, 'flat_number'] = df.loc[msk, 'flat']
    df.loc[msk, 'flat_number'] = df.loc[msk].apply(lambda x: x['flat_number'].strip().replace('flat', '').replace('apartment', ''), axis=1)
    df['flat_number'] = pd.to_numeric(df['flat_number'], errors='coerce')

    # remove those with numbers from flat column - no need to double check
    msk = ~df['flat_number'].isnull()
    df.loc[msk, 'flat'] = None
    df.loc[df['flat'].isnull(), 'flat'] = 'NO'
    df.loc[df['house'].isnull(), 'house'] = 'NO'

    # sometimes road and house has been mushed together - try to split
    msk = df['road'].str.contains(' house ', na=False)
    df.loc[msk, 'house'] = df.loc[msk].apply(_splitRoadHouse, args=(0,), axis=1)
    df.loc[msk, 'road'] = df.loc[msk].apply(_splitRoadHouse, args=(1,), axis=1)

    # some funky postcodes, parser cannot get these because e.g. LZ1 is not valid...
    msk = df['postcode_in'] == 'z1'
    df.loc[msk, 'postcode_in'] = None
    df.loc[msk, 'postcode'] = None
    msk = df['postcode_out'] == '1zz'
    df.loc[msk, 'postcode_out'] = None
    msk = df['postcode_in'] == 'z11'
    df.loc[msk, 'postcode_in'] = None
    df.loc[msk, 'postcode'] = None

    # save for inspection
    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K_parsed.csv', index=False)

    # drop the temp info
    df.drop(['ADDRESS2'], axis=1, inplace=True)

    return df


def matchData(AddressBase, toMatch, limit=0.1):
    """
    Match toMatch data against the AddressBase source information.

    Uses blocking to speed up the matching. This is dangerous for postcodes that
    have been misspelled. For example, if using the full postcode then will miss some addresses.
    Currently uses the incode (the beginning) of the postcode. More appropriate would probably
    be to do an interative approach - first block on full postcode and then for those that no
    matches were found, only block using the incode.

    :param AddressBase: address based dataframe which functions as the source
    :type AddressBase: pandas.DataFrame
    :param toMatch: dataframe holding the address information that is to be matched against a source
    :type toMatch: pandas.DataFrame
    :param limit: the sum of the matching metrics need to be above this limit to count as a potential match.
                  Affects for example the false positive rate.
    :type limit: float

    :return: dataframe with orignal and matched information
    :rtype: pandas.DataFrame
    """
    print('Start matching...')

    # set index names - needed later for merging / duplicate removal
    AddressBase.index.name = 'AB_Index'
    toMatch.index.name = 'EC_Index'

    # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
    pcl = recordlinkage.Pairs(toMatch, AddressBase)
    # pairs = pcl.block('postcode')
    pairs = pcl.block('postcode_in')
    print('\nAfter blocking using incode (first part of postcode), need to test', len(pairs), 'pairs')
    # pairs = pcl.sortedneighbourhood('postcode_in', window=3, block_on='postcode_in')
    # print('\nUsing sorted neighbourhood (windowing) and blocking, need to test', len(pairs), 'pairs')

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)
    compare.string('street_descriptor', 'road', method='damerau_levenshtein', name='street_dl')
    compare.numeric('building_number', 'house_number', threshold=0.01, missing_value=-123, name='number_dl')
    compare.string('town_name', 'city', method='damerau_levenshtein', name='town_dl')
    compare.string('pao_text', 'house', method='damerau_levenshtein', name='pao_dl') #good for care homes
    compare.string('building_name', 'building_name', method='damerau_levenshtein', name='building_name_dl')
    compare.string('postcode', 'postcode', method='damerau_levenshtein', name='postcode_dl')
    # compare.string('postcode_in', 'postcode_in', method='damerau_levenshtein', name='postcode_in_dl')
    compare.exact('postcode_in', 'postcode_in', name='postcode_in_dl')
    # compare.string('postcode_out', 'postcode_out', method='damerau_levenshtein', name='postcode_out_dl')
    compare.exact('postcode_out', 'postcode_out', name='postcode_out_dl')
    compare.string('sao_text', 'flat', method='damerau_levenshtein', name='flat_dl')
    compare.string('locality', 'locality', method='damerau_levenshtein', name='locality_dl')
    compare.string('ORGANISATION', 'house', method='damerau_levenshtein', name='organisation_dl')
    compare.numeric('flat_number', 'flat_number', threshold=0.01, missing_value=-123, name='flat_number_dl')
    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    compare.vectors['postcode_dl'] *= 6.
    compare.vectors['postcode_in_dl'] *= 2.
    compare.vectors['postcode_out_dl'] *= 10.
    compare.vectors['building_name_dl'] *= 5.
    compare.vectors['pao_dl'] *= 4.
    compare.vectors['flat_number_dl'] *= 2.
    compare.vectors['town_dl'] *= 2.
    compare.vectors['street_dl'] *= 5.
    compare.vectors['number_dl'] *= 1.
    compare.vectors['organisation_dl'] *= 4.

    # add sum of the components to the comparison vectors dataframe
    compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)
    # todo: normalise the comparison vectors and the sum to be between zero and unity
    # compare.vectors = compare.vectors.div(compare.vectors.max(axis=1), axis=0)

    # The comparison vectors
    # print(compare.vectors)

    # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
    matches = compare.vectors.loc[compare.vectors['similarity_sum'] > limit]

    # # remove those matches that are likely to be false positives
    # remove = matches['number_dl'] < 1.
    # matches = matches.loc[~remove]

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

    # merge to original information to the matched data
    toMatch = toMatch.reset_index()
    AddressBase = AddressBase.reset_index()
    data = pd.merge(matches, toMatch, how='left', on='EC_Index')
    data = pd.merge(data, AddressBase, how='left', on='AB_Index')

    # drop some unnecessary columns
    data.drop(['EC_Index', 'AB_Index'], axis=1, inplace=True)

    # save to a file
    data.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_matched.csv', index=False)

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

    # find those that were not masked
    uprns = df['uprn_edge'].values
    missing_msk = ~edgeCases['uprn_edge'].isin(uprns)
    missing = edgeCases.loc[missing_msk]
    missing.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EdgeCase_matched_missing.csv', index=False)

    # print out results for each class separtely
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
    print('\nReading in Postcode Data...')
    postcodeinfo = loadPostcodeInformation()

    print('\nReading in Address Base Data...')
    # ab = loadAddressBaseData()
    ab = loadMiniAddressBaseData()

    print('Reading in Edge Case data...')
    edgeCases = loadEdgeCaseTestingData()

    print('Parsing Edge Case data...')
    start = time.clock()
    parsedEdgeCases = parseEdgeCaseData(edgeCases, postcodeinfo)
    stop = time.clock()
    print('\nParsing finished in', round((stop - start), 1), 'seconds...')

    print('Matching Edge Cases to Address Base data...')
    start = time.clock()
    matched = matchData(ab, parsedEdgeCases)
    stop = time.clock()
    print('\nMatching finished in', round((stop - start), 1), 'seconds...')

    print('Checking Performance...')
    checkPerformance(matched, edgeCases)


if __name__ == "__main__":
    runAll()

    """
    This version:
        Matched 4993 entries
        Total Match Fraction 99.9
        Correctly Matched 4674
        Correctly Matched Fraction 93.5
        False Positives 319
        False Positive Rate 6.4
        Correctly Matched 986 CARE_HOMES
        Match Fraction 98.6
        False Positives 14
        False Positive Rate 1.4
        Correctly Matched 1000 DEAD_SIMPLE
        Match Fraction 100.0
        False Positives 0
        False Positive Rate 0.0
        Correctly Matched 822 ORDER_MATTERS
        Match Fraction 82.2
        False Positives 173
        False Positive Rate 17.3
        Correctly Matched 964 PAF_MISMATCH
        Match Fraction 96.4
        False Positives 36
        False Positive Rate 3.6
        Correctly Matched 902 PARTS_MISSING
        Match Fraction 90.2
        False Positives 96
        False Positive Rate 9.6
    """