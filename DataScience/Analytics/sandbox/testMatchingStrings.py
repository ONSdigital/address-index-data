"""

"""
import pandas as pd
import recordlinkage


def testImpactOfNone(fill=False):
    """

    :return:
    """
    # create fake data
    toMatch = pd.DataFrame({'SubBuildingName': ['FLAT 1', None], 'Postcode': ['AA', 'AA']})
    AddressBase = pd.DataFrame({'SubBuildingNameAB': ['FLAT 1', 'FLAT A', None], 'PostcodeAB': ['AA', 'AA', 'AA']})

    if fill:
        msk = toMatch['SubBuildingName'].isnull()
        toMatch.loc[msk, 'SubBuildingName'] = 'NULL'
        msk = AddressBase['SubBuildingNameAB'].isnull()
        AddressBase.loc[msk, 'SubBuildingNameAB'] = 'NULL'

    # create pairs
    pcl = recordlinkage.Pairs(toMatch, AddressBase)

    # set blocking
    pairs = pcl.block(left_on=['Postcode'], right_on=['PostcodeAB'])
    print('Need to test', len(pairs), 'pairs for', len(toMatch.index), 'addresses...')

    # compare the two data sets - use different metrics for the comparison
    compare = recordlinkage.Compare(pairs, AddressBase, toMatch, batch=True)
    compare.string('SubBuildingNameAB', 'SubBuildingName', missing_value=0.1, method='damerau_levenshtein', name='test')
    compare.run()

    # add sum of the components to the comparison vectors dataframe
    compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)

    # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
    matches = compare.vectors.loc[compare.vectors['similarity_sum'] > -1.]

    # sort matches by the sum of the vectors
    matches = matches.sort_values(by='similarity_sum', ascending=False)

    # reset indices
    matches = matches.reset_index()
    toMatch = toMatch.reset_index()
    AddressBase = AddressBase.reset_index()

    # join to the original
    data = pd.merge(matches, toMatch, how='left', left_on='level_0', right_on='index')
    data = pd.merge(data, AddressBase, how='left', left_on='level_1', right_on='index')
    print(data)


if __name__ == "__main__":
    testImpactOfNone()
    print('With Filling:')
    testImpactOfNone(fill=True)
