"""
ONS Address Index - Fuzzy Matching Test
=======================================

A simple example demonstrating fuzzy matching.

Uses e.g. Levenshtein Distance, see here:
https://en.wikipedia.org/wiki/Levenshtein_distance
See also:
http://chairnerd.seatgeek.com/fuzzywuzzy-fuzzy-string-matching-in-python/


Requirements
------------

:requires: pandas
:requires: sqlalchemy
:requires: fuzzywuzzy (https://github.com/seatgeek/fuzzywuzzy)
:requires: recordlinkage (https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 28-Sep-2016
"""
from AddressIndex.Analytics import data
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import recordlinkage
import datetime
import pprint


def getData():
    """
    Get data from the PostGres database

    :return: dataframe containing the queried data
    :rtype: pandas dataframe
    """
    df = data.queryDB('SELECT address FROM addresses')
    print('\nFound', len(df.index), 'addresses...')

    return df


def hammingDistance(s1, s2):
    """
    Return the Hamming distance between equal-length sequences

    :param s1:
    :param s2:

    :return:
    """
    if len(s1) != len(s2):
        raise ValueError("Undefined for sequences of unequal length")

    return sum(el1 != el2 for el1, el2 in zip(s1, s2))


def runSimpleTest1(find_address='6 PROSPECT GARDENS EXTER EX4 6BA'):
    """
    simple test using Levenshtein Distance. Naive solution where the input is tested against all possibilities.

    :return: None
    """
    df = getData()

    test_against_original = df['address'][0]

    print('\nSimple Test:')
    print(test_against_original, 'vs')
    print(find_address)
    print('Ratio:', fuzz.ratio(test_against_original, find_address))
    print('Partial Ratio:', fuzz.partial_ratio(test_against_original, find_address))

    print('\nFinding three best matches for', find_address, '...')
    start = datetime.datetime.now()

    matches = process.extract(find_address, df['address'], limit=3)

    print(matches)
    print('First is correct?', test_against_original == matches[0][0])

    stop = datetime.datetime.now()
    print('\nRun in', round((stop - start).microseconds/1.e6, 2), 'seconds...')


def runSimpleTest2(find_address={'street': ['6 PROSPCT GARDNS EXTER',], 'postcode': ['EX4 6TA',]}):
    """

    :param find_address:
    :return:
    """
    print('Matching')
    pprint.pprint(find_address)

    # get data from the database against which we are linking
    df = getData()
    df['postcode'] = df.apply(data._getPostcode, axis=1)
    df['street'] = df.apply(data._removePostcode, axis=1)
    df.drop(['address', ], axis=1, inplace=True)

    # data frame of the one being linked
    find = pd.DataFrame(find_address)

    print('Start parsed matching with postcode blocking...')
    start = datetime.datetime.now()

    # set blocking
    pcl = recordlinkage.Pairs(df, find)
    pairs = pcl.block('postcode')
    print('\nAfter blocking, need to test', len(pairs))

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, df, find)
    # compare.string('postcode', 'postcode', method='jarowinkler', threshold=0.95, name='postcode')
    # compare.string('street', 'street', method='damerau_levenshtein', threshold=0.85, name='street')
    compare.string('postcode', 'postcode', method='jarowinkler', name='postcode_jw')
    compare.string('street', 'street', method='damerau_levenshtein', name='street_dl')

    # The comparison vectors
    print('\nComparsing vectors:')
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


if __name__ == "__main__":
    runSimpleTest1()
    runSimpleTest2()