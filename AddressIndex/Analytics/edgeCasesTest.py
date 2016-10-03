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
# import chardet
from postal.parser import parse_address
import datetime
import pprint
import re


def loadEdgeCaseTestingData(filename='/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K.csv'):
    # with open(filename, 'rb') as f:
    #     result = chardet.detect(f.read())  # or readline if the file is large
    # print(result)
    #
    # df = pd.read_csv(filename, encoding=result['encoding'])

    df = pd.read_csv(filename)
    print(df.info())

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
    tmp = re.findall(r'[A-PR-UWYZ0-9][A-HK-Y0-9][AEHMNPRTVXY0-9]?[ABEHMNPRVWXY0-9]{1,2}[0-9][ABD-HJLN-UW-Z]{2}|GIR 0AA', row['ADDRESS'])[0]
    inc = tmp[-3:]
    out = tmp.replace(inc, '')
    constructedPostcode = (out + ' ' + inc).lower()

    return constructedPostcode


def loadAddressBaseData():
    pass


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
    addresses = df['ADDRESS'].values
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

    print(df.info())

    # save for inspection
    df.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/EDGE_CASES_EC5K_parsed.csv', index=False)

    return df


def matchData(df, find_address):
    print('Matching')

    # get data from the database against which we are linking

    # data frame of the one being linked
    find = pd.DataFrame(find_address)

    print('Start parsed matching with postcode blocking...')
    start = datetime.datetime.now()

    # set blocking
    pcl = recordlinkage.Pairs(df, find)
    pairs = pcl.block('postcode')
    print('\nAfter blocking, need to test', len(pairs))

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, df, find, batch=True)
    compare.string('street', 'street', method='damerau_levenshtein', name='street_dl')
    compare.run()

    # The comparison vectors
    print('\nComparison vectors:')
    print(compare.vectors)

    # find the matches and the best match
    matchmetrics = compare.vectors.sum(axis=1)
    potentialMatches = matchmetrics.index.levels[0].tolist()
    print('\nPotential Matches:')
    print(df.loc[potentialMatches])
    print('\nBest Match:')
    print(df.loc[matchmetrics.argmax()[0]])

    stop = datetime.datetime.now()
    print('\nRun in', round((stop - start).microseconds/1.e6, 2), 'seconds...')



def checkPerformance():
    pass


def runAll():
    edgeCases = loadEdgeCaseTestingData()
    parsedEdgeCases = parseEdgeCaseData(edgeCases)


if __name__ == "__main__":
    runAll()