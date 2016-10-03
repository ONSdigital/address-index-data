"""
ONS Address Index - Edge Case Testing
=====================================

A simple script to test parsing and matching of edge cases.


Requirements
------------

:requires: pandas
:requires: libpostal (https://github.com/openvenues/libpostal)
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 3-Oct-2016
"""
import pandas as pd
import recordlinkage
from postal.parser import parse_address
import datetime
import pprint
import re
from AddressIndex.Analytics import data


def loadEdgeCaseTestingData(filename='/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K.csv',
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


def getPostcode(string):
    """
    Extract a postcode from address information.

    Uses regular expression to extract the postcode:
    http://regexlib.com/REDetails.aspx?regexp_id=260&AspxAutoDetectCookieSupport=1

    :param string: string to be parsed
    :type string: str

    :return: postcode
    :rtype: str
    """
    try:
        tmp = re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]? {1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA', string)[0]
        tmp = tmp.lower()
    except:
        tmp = None

    return tmp


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


def loadAddressBaseData():
    """

    :return:
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


def loadMiniAddressBaseData():
    """

    :return:
    """
    path = '/Users/saminiemi/Projects/ONS/AddressIndex/data/miniAB/'
    df = pd.read_csv(path + 'combined.csv')

    # convert everything to lower case
    for tmp in df.columns:
        try:
            df[tmp] = df[tmp].str.lower()
        except:
            pass

    # change column names
    df.rename(columns={'POSTCODE_LOCATOR': 'postcode', 'STREET_DESCRIPTOR': 'street_descriptor',
                       'TOWN_NAME': 'town_name', 'BUILDING_NUMBER': 'number', 'PAO_TEXT': 'pao_text'}, inplace=True)

    return df


def parseEdgeCaseData(df):
    """
    Parses the address information from the edge case data. Examples:

    Difficult and incorrectly parsed:
    [('exbury', 'road'), ('place', 'house'), ('12-13', 'house_number'), ('exbury place', 'road'), ('st peters', 'suburb'), ('worcester', 'city'), ('wr5 3tp', 'postcode')]

    Easy:
    ('mount pleasant care home', 'house'), ('18', 'house_number'), ('rosemundy', 'road'), ('st agnes', 'city'), ('tr5 0ud', 'postcode')]

    No city (london borough of hillingdon):
    [('life opportunities trust', 'house'), ('13', 'house_number'), ('devon way', 'road'), ('hillingdon', 'suburb'), ('ub10 0js', 'postcode')]

    :param df: pandas dataframe containing ADDRESS column that is being parsed
    :type df: pandas.DataFrame

    :return: pandas dataframe where the parsed information has been inclsuded
    :rtype: pandas.DataFrame
    """
    df['ADDRESS2'] = df['ADDRESS'].copy()
    # parsing gets really confused if region or county is in the line
    # for a quick hack I remove these, but regions should probably be part of the training
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
    # postal counties
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('NORTH HUMBERSIDE', ''), axis=1)
    df['ADDRESS2'] = df.apply(lambda x: x['ADDRESS2'].replace('SOUTH HUMBERSIDE', ''), axis=1)


    # get addresses - the only ones needed
    addresses = df['ADDRESS2'].values
    print('Parsing', len(addresses), 'addresses...')

    # temp data storage
    postcodes = []
    house_number = []
    house = []
    road = []
    city = []

    # loop over addresses - todo: quite in efficient, should avoid a loop
    for address in addresses:
        parsed = parse_address(address) # probabilistic
        pcode = getPostcode(address) # regular expression

        #print(parsed)

        store = {}

        for tmp in parsed:

            if tmp[1] == 'city':
                store['city'] = tmp[0]

            if tmp[1] == 'house_number':
                store['house_number'] = tmp[0]

            if tmp[1] == 'house':
                store['house'] = tmp[0]

            if tmp[1] == 'road':
                store['road'] = tmp[0]

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

        city.append(store.get('city', None))
        house_number.append(store.get('house_number', None))
        house.append(store.get('house', None))
        road.append(store.get('road', None))
        postcodes.append(store.get('postcode', None))

    # add the parsed information to the dataframe
    df['postcode'] = postcodes
    df['house_number'] = house_number
    df['house'] = house
    df['road'] = road
    df['city'] = city

    # for those without postcode, we need to make another pass as it might be in the address but in wrong format
    noPostcode = pd.isnull(df['postcode'])
    df['postcode'].loc[noPostcode] = df.loc[noPostcode].apply(getIllformattedPostcode, axis=1)

    # todo: remove anything that looks like a postcode in the other fields

    # print(df.info())

    # save for inspection
    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K_parsed.csv', index=False)

    # drop the temp info
    df.drop(['ADDRESS2'], axis=1, inplace=True)

    return df


def matchData(AddressBase, toMatch, limit=0.7):
    """

    :param AddressBase:
    :param toMatch:
    :return:
    """
    print('Start matching with postcode blocking...')

    # set index names - needed later for merging
    AddressBase.index.name = 'AB_Index'
    # AddressBase.index.key = 'AB'
    toMatch.index.name = 'EC_Index'
    # toMatch.index.key = 'EC'

    # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
    pcl = recordlinkage.Pairs(toMatch, AddressBase)
    pairs = pcl.block('postcode')
    print('\nAfter blocking, need to test', len(pairs), 'pairs')

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)
    compare.string('street_descriptor', 'road', method='damerau_levenshtein', name='street_dl')
    compare.string('number', 'house_number', method='damerau_levenshtein', name='number_dl') # todo: something better?
    compare.string('town_name', 'city', method='damerau_levenshtein', name='town_dl')
    compare.string('pao_text', 'house', method='damerau_levenshtein', name='pao_dl')
    compare.run()

    # arbitrarily scale up some of the comparisons
    # compare.vectors['number_dl'] *= 3.
    # compare.vectors['town_dl'] *= 2.
    # compare.vectors['street_dl'] *= 1.5

    # The comparison vectors
    # print('\nComparison vectors:')
    # print(compare.vectors)

    # find all matches where the metrics is above the chosen limit
    matches = compare.vectors.loc[compare.vectors.sum(axis=1) > limit]
    # todo: need to resolve those that match to multiple - pick the best!

    print('Found ', len(matches), 'matches...')

    # merge the original data to the multi-index dataframe
    data = matches.merge(toMatch, left_index=True, right_index=True, how='inner')
    data = data.merge(AddressBase, left_index=True, right_index=True, how='inner')

    # save to a file
    data.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/merged.csv', index=True)

    return data


def checkPerformance(df, edgeCases):
    nmatched = len(df.index)
    all = len(edgeCases.index)

    print('Matched', nmatched, 'entries')
    print('Match Fraction', round(nmatched / all * 100., 1),)

    msk = df['UPRN'] == df['uprn_edge']
    correct = df.loc[msk]
    print('Correctly Matched', len(correct.index))

    for mnemonic in set(df['MNEMONIC'].values):
        msk = (df['UPRN'] == df['uprn_edge']) & (df['MNEMONIC'] == mnemonic)
        correct = df.loc[msk]
        nmatched = len(correct.index)
        outof = len(edgeCases.loc[edgeCases['MNEMONIC'] == mnemonic].index)

        print('Correctly Matched', nmatched, mnemonic)
        print('Match Fraction', round(nmatched / outof *100., 2))

    # confusion matrix
    # recordlinkage.confusion_matrix()
    # precision
    # recall
    # f-score
    # number of false positives and fp rate


def runAll():
    print('\nReading in Address Base Data...')
    # ab = loadAddressBaseData()
    ab = loadMiniAddressBaseData()

    print('Reading in Edge Case data...')
    edgeCases = loadEdgeCaseTestingData()

    print('Parsing Edge Case data...')
    start = datetime.datetime.now()
    parsedEdgeCases = parseEdgeCaseData(edgeCases)
    stop = datetime.datetime.now()
    print('\nFinished in', round((stop - start).microseconds / 1.e3, 2), 'milliseconds...')

    print('Matching Edge Cases to Address Base data...')
    start = datetime.datetime.now()
    matched = matchData(ab, parsedEdgeCases)
    stop = datetime.datetime.now()
    print('\nFinished in', round((stop - start).microseconds / 1.e3, 2), 'milliseconds...')

    print('Checking Performance...')
    checkPerformance(matched, edgeCases)


if __name__ == "__main__":
    runAll()