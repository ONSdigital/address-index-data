#!/usr/bin/env python
"""
ONS Address Index - Welsh Addresses Test
========================================

A simple script to attach UPRNs to Welsh test data.

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.


Running
-------

TODO

Requirements
------------

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: pandas
:requires: numpy
:requires: matplotlib
:requires: tqdm (https://github.com/tqdm/tqdm)
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 18-Nov-2016


Results
-------

With full AB and reasonable runtime (i.e. using blocking):
    Total Match Fraction
"""
import pandas as pd
import numpy as np
import recordlinkage
from ProbabilisticParser import parser
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns
import time
import re
import datetime


def loadData(filename='WelshGovernmentData21Nov2016.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
             verbose=False):
    """
    Read in the Welsh address test data.

    :param filename: name of the CSV file holding the data
    :type filename: str
    :param path: location of the test data
    :type path: str
    :param verbose: whether or not output information
    :type verbose: bool

    :return: pandas dataframe of the data
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv(path + filename, low_memory=False)

    # fill NaNs with empty strings so that we can form a single address string
    df.fillna('', inplace=True)
    df['ADDRESS'] = df['Building'] + ' ' + df['Street'] + ' ' + df['Locality'] + ' ' + df['Town'] +\
                    ' ' + df['County'] + ' ' + df['Postcode']

    # rename postcode to postcode_orig and locality to locality_orig
    df.rename(columns={'UPRNs_matched_to_date': 'UPRN_prev'}, inplace=True)

    if verbose:
        print(df.info())

    print('Found', len(df.index), 'addresses...')

    return df


def loadAddressBaseData(filename='AB.csv', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/'):
    """
    Load a compressed version of the full AddressBase file. The information being used
    has been processed from a AB Epoch 39 files provided by ONS.

    .. Note: this function modifies the original AB information by e.g. combining different tables. Such
             activities are undertaken because of the aggressive blocking the prototype linking code uses.
             The actual production system should take AB as it is and the linking should not perform blocking
             but rather be flexible and take into account that in NAG the information can be stored in various
             fields.

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

    msk = df['POST_TOWN'].isnull()
    df.loc[msk, 'POST_TOWN'] = df.loc[msk, 'TOWN_NAME']

    msk = df['LOCALITY'].isnull()
    df.loc[msk, 'LOCALITY'] = df.loc[msk, 'DEPENDENT_LOCALITY']

    # some addresses might have PAO_START_NUMBER but not BUILDING_NUMBER, e.g.
    # 23 SUNNINGDALE CLOSE  NORTHAMPTON NN2 7LR is found in NAG under PLOT 4.
    # Others e.g. 13 HOME RIDINGS HOUSE FLINTERGILL COURT HEELANDS MILTON KEYNES MK13 7QS does
    # not contain any building number as the 13 is part of building_name (parsed correctly)
    msk = df['BUILDING_NUMBER'].isnull()
    df.loc[msk, 'BUILDING_NUMBER'] = df.loc[msk, 'PAO_START_NUMBER']

    # drop some that are not needed
    df.drop(['DEPENDENT_LOCALITY', 'POSTCODE_LOCATOR'], axis=1, inplace=True)

    # split postcode to in and outcode, useful in different ways of performing blocking
    pcodes = df['POSTCODE'].str.split(' ', expand=True)
    pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
    df = pd.concat([df, pcodes], axis=1)

    # rename some columns (sorted windowing requires column names to match)
    df.rename(columns={'THROUGHFARE': 'StreetName',
                       'POST_TOWN': 'townName',
                       'POSTCODE': 'postcode',
                       'PAO_TEXT': 'pao_text',
                       'LOCALITY': 'locality',
                       'BUILDING_NAME': 'buildingName'}, inplace=True)

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
    except IndexError:
        tmp = None

    # above regex gives also those without space between, add if needed
    if tmp is not None:
        if ' ' not in tmp:
            inc = tmp[-3:]
            out = tmp.replace(inc, '')
            tmp = out + ' ' + inc

    return tmp


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

    # remove spaces around hyphens as this causes ranges to be interpreted incorrectly
    # e.g. FLAT 15 191 - 193 NEWPORT ROAD  CARDIFF CF24 1AJ is parsed incorrectly if there
    # is space around the hyphen
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace(' - ', '-'), axis=1)

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
                (' GLOS(?:\s|\Z)', ' GLOUCESTERSHIRE '),
                (' STOKE ON TRENT ', ' STOKE-ON-TRENT '),
                (' SOUTHEND ON SEA ', ' SOUTHEND-ON-SEA '),
                (' WESTCLIFF ON SEA ', ' WESTCLIFF-ON-SEA '),
                (' ENGLAND(?:\s|\Z)', ' '),
                (' UNITED KINGDOM(?:\s|\Z)', ' '),
                (' 1ST ', ' FIRST '),
                (' 2ND ', ' SECOND '),
                (' 3RD ', ' THIRD '),
                (' 4TH ', ' FOURTH '),
                (' 5TH ', ' FIFTH '),
                (' 6TH ', ' SIXTH '),
                (' 7TH ', ' SEVENTH '),
                (' 8TH ', ' EIGHT ')]

    # expand common synonyms to help with parsing
    if expandSynonyms:
        print('Expanding synonyms as a part of normalisation...')
        for fro, to in synonyms:
            df['ADDRESS2'] = df['ADDRESS2'].str.replace(fro, to)

    # parsing gets really confused if region or county is in the line
    counties = ('WEST MIDLANDS', 'WEST YORKSHIRE', 'S YORKSHIRE', 'N YORKSHIRE', 'W YORKSHIRE', 'W SUSSEX',
                'E SUSSEX', 'KENT', 'SOUTH GLAMORGAN', 'MID GLAMORGAN', 'WEST GLAMORGAN', ' ESSEX', 'SURREY', 'SUFFOLK',
                'CHESHIRE', 'CARMARTHENSHIRE', 'DERBYSHIRE', 'BERKSHIRE', 'YORKSHIRE', 'HEREFORDSHIRE', 'LINCOLNSHIRE',
                'NOTTINGHAMSHIRE', 'OXFORDSHIRE', 'BUCKINGHAMSHIRE', 'SHROPSHIRE', 'DORSET', 'DEVON', 'SOMERSET',
                'CORNWALL', 'CLEVELAND', 'NORFOLK', 'STAFFORDSHIRE', 'MIDDLESEX', 'MERSEYSIDE', 'NORTH HUMBERSIDE',
                'SOUTH HUMBERSIDE', 'ISLE OF WIGHT', 'CUMBRIA', 'FLINTSHIRE', 'GLOUCESTERSHIRE', 'WILTSHIRE',
                'DENBIGHSHIRE', 'TYNE AND WEAR', 'NORTHUMBERLAND', 'NORTHAMPTONSHIRE', 'WARWICKSHIRE', 'HAMPSHIRE',
                'GWENT', 'NORFOLK', 'CHESHIRE', 'POWYS', 'LEICESTERSHIRE', 'NORTHAMPTONSHIRE', 'NORTHANTS',
                'WORCESTERSHIRE', 'HERTFORDSHIRE', 'CAMBRIDGESHIRE', 'BEDFORDSHIRE', 'LANCASHIRE')

    # use this for the counties so that e.g. ESSEX ROAD does not become just ROAD...
    # todo: the regex is getting ridiculous, maybe do other way around i.e. country must be followed by postcode or
    #       be the last component.
    addRegex = '(?:\s)(?!ROAD|LANE|STREET|CLOSE|DRIVE|AVENUE|SQUARE|COURT|PARK|CRESCENT|WAY|WALK|HEOL|FFORDD|HILL|GARDENS|GATE|GROVE|HOUSE|VIEW|BUILDING|VILLAS|LODGE|PLACE|ROW|WHARF|RISE|TERRACE|CROSS|ENTERPRISE|HATCH)'

    # remove county from address but add a column for it
    df['County'] = None
    for county in counties:
        msk = df['ADDRESS2'].str.contains(county + addRegex, regex=True, na=False)
        df.loc[msk, 'County'] = county
        df['ADDRESS2'] = df['ADDRESS2'].str.replace(county + addRegex, '', case=False)

    return df


def _fixLondonBoroughs(parsed):
    """
    A method to address incorrectly parsed London boroughs.
    If the street name contains London borough then move it to locality and remove from the street name.

    :param parsed: a dictionary containing the address tokens that have been parsed
    :type parsed: dict

    :return:
    """
    # todo: should move to a file rather than have inside the code and get a complete list from AB
    locs = ['HACKNEY', 'ISLINGTON', 'STRATFORD', 'EAST HAM', 'WOOD GREEN', 'FINCLEY', 'HORNSEY', 'HENDON',
            'TOTTENHAM', 'BLACKHEATH', 'BAYSWATER', 'CHISWICK VILLAGE', 'CHISWICK', 'COLINDALE', 'LEWISHAM',
            'FOREST HILL', 'NORBURY', 'MANOR PARK', 'PLAISTOW', 'ABBEY WOOD', 'SOUTH NORWOOD', 'CHARLTON',
            'MOTTINGHAM', 'NEW ELTHAM', 'BATTERSEA', 'PUTNEY', 'TOOTING', 'RAYNES PARK', 'MORTLAKE',
            'WEST KENSINGTON', 'KENSINGTON', 'ACTON', 'HAMMERSMITH', 'HANWELL', 'NEW SOUTHGATE',
            'GREEN LANES', 'STREATHAM HILL', 'CATFORD', 'LEWISHAM', 'BALHAM', 'OLYMPIC PARK', 'CHINGFORD',
            'STREATHAM', 'LEYTONSTONE', 'BROCKLEY', 'SOUTH WALTHAMSTOW', 'WALTHAMSTOW', 'MAIDA VALE',
            'HOLLAND PARK', 'FULHAM', 'PARK ROYAL', 'LEYTON', 'TULSE HILL', 'SILVERTOWN', 'WOODFORD',
            'ROYAL VICTORIA DOCK', 'CROUCH END', 'EDMONTON', 'PLUMSTEAD', 'ELTHAM', 'EAST DULWICH',
            'MUSWELL HILL', 'EALING', 'WANSTEAD', 'WIMBLEDON', 'UPPER NORWOOD', 'CAMBERWELL', 'SYDENHAM',
            'SOUTHFIELDS', 'COLLIERS WOOD', 'THAMESMEAD', 'WILLESDEN', 'HAMPSTEAD', 'KILBURN',
            'KENTISH TOWN', 'HARLESDEN', 'FULHAM', 'WEST DULWICH', 'LONDON', 'PALMERS GREEN', 'MARYLEBONE',
            'CLAPHAM', 'WANDSWORTH', 'WOOLWICH', 'BELLINGHAM', 'GREENWICH', 'NEW CROSS', 'KIDBROOKE',
            'HOMERTON']

    for loc in locs:
        if parsed['StreetName'].strip().endswith(loc):
            parsed['Locality'] = loc
            # take the last part out, so that e.g. CHINGFORD AVENUE CHINGFORD is correctly processed
            # need to be careful with e.g.  WESTERN GATEWAY ROYAL VICTORIA DOCK (3 parts to remove)
            parsed['StreetName'] = parsed['StreetName'].strip()[:-len(loc)].strip()

    return parsed



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
    for address in tqdm(addresses):
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

        # if Hackney etc. in StreetName then remove and move to locality if town name contains London
        # todo: probabilistic parser should see more cases with london localities, parsed incorrectly at the mo
        if parsed.get('StreetName', None) is not None and parsed.get('TownName', None) is not None:
            if 'LONDON' in parsed['TownName']:
                parsed = _fixLondonBoroughs(parsed)

        # if BuildingName is e.g. 55A then should get the number and suffix separately
        if parsed.get('BuildingName', None) is not None:
            parsed['BuildingSuffix'] = ''.join([x for x in parsed['BuildingName'] if not x.isdigit()])
            # accept suffixes that are only maximum two chars
            if len(parsed['BuildingSuffix']) > 2:
                parsed['BuildingSuffix'] = None

        # some addresses contain place CO place, where the CO is not part of the actual name - remove these
        # same is true for IN e.g. Road Marton IN Cleveland
        if parsed.get('Locality', None) is not None:
            if parsed['Locality'].strip().endswith(' CO'):
                parsed['Locality'] = parsed['Locality'].replace(' CO', '')
            if parsed['Locality'].strip().endswith(' IN'):
                parsed['Locality'] = parsed['Locality'].replace(' IN', '')

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

    # # split flat or apartment number as separate for numerical comparison - compare e.g. SAO number
    df['FlatNumber'] = None
    msk = df['SubBuildingName'].str.contains('flat|apartment', na=False, case=False)
    df.loc[msk, 'FlatNumber'] = df.loc[msk, 'SubBuildingName']
    df.loc[msk, 'FlatNumber'] = df.loc[msk].apply(lambda x:
                                                  x['FlatNumber'].strip().replace('FLAT', '').replace('APARTMENT', ''),
                                                  axis=1)
    df['FlatNumber'] = pd.to_numeric(df['FlatNumber'], errors='coerce')

    # save for inspection
    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/ParsedAddresses.csv', index=False)

    # drop the temp info
    df.drop(['ADDRESS2', ], axis=1, inplace=True)

    return df


def matchDataWithPostcode(AddressBase, toMatch, limit=0.1, buildingNumberBlocking=True):
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
    :param buildingNumberBlocking: whether or not to block on BuildingNumber of BuildingName
    :type buildingNumberBlocking: bool

    :return: dataframe of matches
    :rtype: pandas.DataFrame
    """
    # create pairs
    pcl = recordlinkage.Pairs(toMatch, AddressBase)

    # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
    # block on both postcode and house number, street name can have typos and therefore is not great for blocking
    if buildingNumberBlocking:
        # print('Start matching those with postcode information, using postcode blocking...')
        # pairs = pcl.block(left_on=['Postcode'], right_on=['postcode'])
        print('Start matching those with postcode information, using postcode and building number blocking...')
        pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'], right_on=['postcode', 'BUILDING_NUMBER'])
    else:
        print('Start matching those with postcode information, using postcode blocking...')
        pairs = pcl.block(left_on=['Postcode'], right_on=['postcode'])

    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets
    # the idea is to build evidence to support linking, hence some fields are compared multiple times
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)

    # set rules for standard residential addresses
    compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl')
    compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl')
    compare.string('buildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
    compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl')
    compare.string('StreetName', 'StreetName', method='damerau_levenshtein', name='street_dl')
    compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl')
    compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl')

    # the following is good for flats and apartments than have been numbered
    compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl')
    compare.string('SAO_START_NUMBER', 'FlatNumber', method='damerau_levenshtein', name='sao_number_dl')
    # some times the PAO_START_NUMBER is 1 for the whole house without a number and SAO START NUMBER refers
    # to the flat number, but the flat number is actually part of the house number without flat/apt etc. specifier
    # This comparison should probably be numeric.
    compare.string('SAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='sao_number2_dl')

    # sometimes when there is no street name, the parser sets the building name to street name
    compare.string('buildingName', 'StreetName', method='damerau_levenshtein', name='street_building_dl')

    # set rules for organisations such as care homes and similar type addresses
    compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl')

    # execute the comparison model
    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    compare.vectors['pao_dl'] *= 5.
    compare.vectors['sao_number_dl'] *= 4.
    compare.vectors['flat_dl'] *= 3.
    compare.vectors['building_name_dl'] *= 3.
    compare.vectors['street_building_dl'] *= 3.

    # add sum of the components to the comparison vectors dataframe
    compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)

    # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
    matches = compare.vectors.loc[compare.vectors['similarity_sum'] > limit]

    # to pick the most likely match we sort by the sum of the similarity and pick the top
    # sort matches by the sum of the vectors and then keep the first
    matches = matches.sort_values(by='similarity_sum', ascending=False)

    # reset index
    matches = matches.reset_index()

    # keep first if duplicate in the WG_Index column
    matches = matches.drop_duplicates('WG_Index', keep='first')

    # sort by WG_Index
    matches = matches.sort_values(by='WG_Index')

    print('Found ', len(matches.index), 'matches...')

    return matches


def matchDataNoPostcode(AddressBase, toMatch, limit=0.7, buildingNumberBlocking=True):
    """
    Link toMatch data to the AddressBase source information.
    Uses blocking to speed up the matching.

    .. note: the aggressive blocking that uses street name rules out any addresses with a typo in the street name.
             Given that this is fairly common, one should use sorted neighbourhood search instead of blocking.
             However, given that this prototype needs to run on a laptop with limited memory in a reasonable time
             the aggressive blocking is used. In production, no blocking should be used.

    :param AddressBase: AddressBase dataframe which functions as the source
    :type AddressBase: pandas.DataFrame
    :param toMatch: dataframe holding the address information that is to be matched against a source
    :type toMatch: pandas.DataFrame
    :param limit: the sum of the matching metrics need to be above this limit to count as a potential match.
                  Affects for example the false positive rate.
    :type limit: float
    :param buildingNumberBlocking: whether or not to block on BuildingNumber of BuildingName
    :type buildingNumberBlocking: bool

    :return: dataframe of matches
    :rtype: pandas.DataFrame
    """
    # create pairs
    pcl = recordlinkage.Pairs(toMatch, AddressBase)

    # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
    if buildingNumberBlocking:
        print('Start matching those without postcode information, using building number and street name blocking...')
        pairs = pcl.block(left_on=['BuildingNumber', 'StreetName'], right_on=['BUILDING_NUMBER', 'StreetName'])
    else:
        print('Start matching those without postcode information, using building name and street name blocking...')
        pairs = pcl.block(left_on=['BuildingName', 'StreetName'], right_on=['buildingName', 'StreetName'])

    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets - use different metrics for the comparison
    # the idea is to build evidence to support linking, hence some fields are compared multiple times
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)

    compare.string('SAO_START_NUMBER', 'FlatNumber', method='damerau_levenshtein', name='sao_number_dl')
    compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl')
    if ~buildingNumberBlocking:
        compare.string('buildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
    compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl')
    compare.string('StreetName', 'StreetName', method='damerau_levenshtein', name='street_dl')
    compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl')
    compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl')
    # add a comparison from other fields
    compare.string('TOWN_NAME', 'TownName', method='damerau_levenshtein', name='town2_dl')

    # the following is good for flats and apartments than have been numbered
    compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl')
    compare.string('SAO_START_NUMBER', 'FlatNumber', method='damerau_levenshtein', name='sao_number_dl')

    # set rules for organisations such as care homes and similar type addresses
    compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl')
    compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl')
    compare.string('ORGANISATION_NAME', 'OrganisationName', method='damerau_levenshtein', name='org2_dl')
    compare.string('DEPARTMENT_NAME', 'DepartmentName', method='damerau_levenshtein', name='department_dl')

    # Extras
    compare.string('STREET_DESCRIPTOR', 'StreetName', method='damerau_levenshtein', name='street_desc_dl')

    compare.run()

    # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
    compare.vectors['pao_dl'] *= 5.
    compare.vectors['town_dl'] *= 7.
    compare.vectors['organisation_dl'] *= 4.
    compare.vectors['sao_number_dl'] *= 4.
    compare.vectors['flat_dl'] *= 3.
    compare.vectors['building_name_dl'] *= 3.
    compare.vectors['locality_dl'] *= 2.

    # add sum of the components to the comparison vectors dataframe
    compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)

    # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
    matches = compare.vectors.loc[compare.vectors['similarity_sum'] > limit]

    # to pick the most likely match we sort by the sum of the similarity and pick the top
    # sort matches by the sum of the vectors and then keep the first
    matches = matches.sort_values(by='similarity_sum', ascending=False)

    # reset index
    matches = matches.reset_index()

    # keep first if duplicate in the WG_Index column
    matches = matches.drop_duplicates('WG_Index', keep='first')

    # sort by WG_Index
    matches = matches.sort_values(by='WG_Index')

    print('Found ', len(matches.index), 'matches...')

    return matches


def mergeMatchedAndAB(matches, toMatch, AddressBase, dropColumns=False):
    """
    Merge address base information to the identified matches.
    Outputs the merged information to a CSV file for later inspection.

    :param matches: found matches
    :type matches: pandas.DataFrame
    :param toMatch: addresses that were being matched
    :type toMatch: pandas.DataFrame
    :param AddressBase: original AddressBase dataframe
    :type AddressBase: pandas.DataFrame

    :return: merged dataframe
    :rtype: pandas.DataFrame
    """
    # merge to original information to the matched data
    toMatch = toMatch.reset_index()
    data = pd.merge(matches, toMatch, how='left', on='WG_Index')
    data = pd.merge(data, AddressBase, how='left', on='AB_Index')

    # drop unnecessary columns
    if dropColumns:
        data.drop(['WG_Index', 'AB_Index'], axis=1, inplace=True)

    return data


def checkPerformance(df, linkedData,
                     prefix='WelshGov', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/'):
    """
    Check performance - calculate the match rate.

    :param df: data frame with linked addresses and similarity metrics
    :type df: pandas.DataFrame
    :param linkedData: input edge case data, used to identify e.g. those addresses not linked
    :type linkedData: pandas.DataFrame
    :param prefix: prefix name for the output files
    :type prefix: str
    :param path: location where to store the output files
    :type path: str

    :return: None
    """
    # count the number of matches and number of edge cases
    nmatched = len(df.index)
    total = len(linkedData.index)

    # how many were matched
    print('\nMatched', nmatched, 'entries')
    print('Total Match Fraction', round(nmatched / total * 100., 1), 'per cent')

    # save matched
    df.to_csv(path + prefix + '_matched.csv', index=False)

    # find those without match
    IDs = df['ID'].values
    missing_msk = ~linkedData['ID'].isin(IDs)
    missing = linkedData.loc[missing_msk]
    missing.to_csv(path + prefix + '_matched_missing.csv', index=False)
    print(len(missing.index), 'addresses were not linked...')

    # find those with UPRN attached earlier and check which are the same and which are different
    earlierUPRNs = df['UPRN_prev'].values
    msk = linkedData['UPRN'].isin(earlierUPRNs)
    matches = linkedData.loc[msk]
    nonmatches = linkedData.loc[~msk]
    matches.to_csv(path + prefix + '_sameUPRN.csv', index=False)
    nonmatches.to_csv(path + prefix + '_differentUPRN.csv', index=False)

    print(len(matches.index), 'addresses have the same UPRN is earlier...')
    print(len(nonmatches.index), 'addresses have a different UPRN is earlier...')


def runAll():
    """
    Run all required steps.

    :return: None
    """
    print('\nReading in Address Base Data...')
    start = time.clock()
    ab = loadAddressBaseData()
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nReading in Welsh Government data...')
    start = time.clock()
    testData = loadData()
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nParsing address data...')
    start = time.clock()
    parsedAddresses = parseInputData(testData)
    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    # set index names - needed later for merging / duplicate removal
    ab.index.name = 'AB_Index'
    parsedAddresses.index.name = 'WG_Index'

    # split to those with full postcode and no postcode - use different matching strategies
    msk = parsedAddresses['Postcode'].isnull()
    withPC = parsedAddresses.loc[~msk]
    noPC = parsedAddresses.loc[msk]

    print('\nMatching addresses against Address Base data...')
    start = time.clock()
    ms1 = ms2 = m1a = m1b = m2a = m2b = False
    if len(withPC.index) > 0:
        msk = withPC['BuildingNumber'].isnull()
        withPCnoHouseNumber = withPC.loc[msk]
        withPCHouseNumber = withPC.loc[~msk]
        if len(withPCHouseNumber) > 0:
            matches1a = matchDataWithPostcode(ab, withPCHouseNumber, buildingNumberBlocking=True)
            m1a = True
        if len(withPCnoHouseNumber) > 0:
            matches1b = matchDataWithPostcode(ab, withPCnoHouseNumber, buildingNumberBlocking=False)
            m1b = True
        ms1 = True
    if len(noPC.index) > 0:
        msk = noPC['BuildingNumber'].isnull()
        noPCnoHouseNumber = noPC.loc[msk]
        noPCHouseNumber = noPC.loc[~msk]
        if len(noPCnoHouseNumber) > 0:
            matches2a = matchDataNoPostcode(ab, noPCHouseNumber, buildingNumberBlocking=True)
            m2a = True
        if len(noPCHouseNumber) > 0:
            matches2b = matchDataNoPostcode(ab, noPCnoHouseNumber, buildingNumberBlocking=False)
            m2b = True
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
            matched1a = mergeMatchedAndAB(matches1a, parsedAddresses, ab)
        if m1b:
            matched1b = mergeMatchedAndAB(matches1b, parsedAddresses, ab)
        m1 = True
    if ms2:
        if m2a:
            matched2a = mergeMatchedAndAB(matches2a, parsedAddresses, ab)
        if m2b:
            matched2b = mergeMatchedAndAB(matches2b, parsedAddresses, ab)
        m2 = True

    if m1a & m1b:
        matched1 = matched1a.append(matched1b)
    elif m1a:
        matched1 = matched1a
    elif m1b:
        matched1 = matched1b

    if m2a & m2b:
        matched2 = matched2a.append(matched2b)
    elif m2a:
        matched2 = matched2a
    elif m2b:
        matched2 = matched2b

    if m2 & m1:
        matched = matched1.append(matched2)
    elif m1:
        matched = matched1
    elif m2:
        matched = matched2

    stop = time.clock()
    print('finished in', round((stop - start), 1), 'seconds...')

    print('\nChecking Performance...')
    checkPerformance(matched, testData)


if __name__ == "__main__":
    runAll()
