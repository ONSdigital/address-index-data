"""
ONS Address Index - Fuzzy Matching Test
=======================================

A simple example demonstrating fuzzy matching.

Uses Levenshtein Distance, see here:
https://en.wikipedia.org/wiki/Levenshtein_distance


Requirements
------------

:requires: pandas
:requires: sqlalchemy
:requires: fuzzywuzzy (https://github.com/seatgeek/fuzzywuzzy)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 27-Sep-2016
"""
from AddressIndex.Analytics import data
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


def getData():
    testQuery = 'SELECT address FROM addresses'
    # pull data from db
    df = data.queryDB(testQuery)
    print('found', len(df.index), 'addresses...')

    return df


def runTest():
    data = getData()

    address = data['address'][0]
    test_address = '6 PROSPECT GARDENS EXTER EX4 6BA'

    print('Simple Test:')
    print(address, 'vs', test_address)
    print('Ratio:', fuzz.ratio(address, test_address))
    print('Partial Ratio:', fuzz.partial_ratio(address, test_address))

    print('Finding three best matches for', test_address, '...')
    matches = process.extract(test_address, data['address'], limit=3)
    print(matches)
    print('First is correct?', address == matches[0][0])


if __name__ == "__main__":
    runTest()
