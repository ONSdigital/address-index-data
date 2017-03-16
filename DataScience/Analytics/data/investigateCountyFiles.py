#!/usr/bin/env python
"""
ONS Address Index - Investigating Different County Lists
========================================================

A simple script to compare two county lists i.e. to find the union, intersect, and complements.


Running
-------

After all requirements are satisfied and the datasets are available, the script can be run using CPython::

    python investigateCountyFiles.py


Requirements
------------

:requires: pandas (tested with 0.19.2)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 16-Mar-2017
"""
import pandas as pd


def _normalize(df):
    """
    Normalise the county data i.e. strip spaces and convert to upper case, and finally sort alphabetically.

    :param df: input data frame with county column
    :type df: pandas.DataFrame

    :return: normalised data
    :rtype: pandas.DataFrame
    """
    df.sort_values(by='county', inplace=True)
    df['county'] = df['county'].str.upper()
    df['county'] = df['county'].str.strip()

    return df


def read_original_county_file():
    """
    Read in original county file that is present in the repository.
    Normalises the county information.

    :return: counties in a dataframe
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv('../../data/counties.csv')
    df = _normalize(df)

    print(len(df.index), 'counties in the original file')

    return df


def read_paul_county_file():
    """
    Read in Paul's county file. Normalises the county information.

    :return: counties in a dataframe
    :rtype: pandas.DataFrame
    """
    df = pd.read_csv('/Users/saminiemi/Dropbox/ONS/County_List_PG.csv')
    df = _normalize(df)

    print(len(df.index), 'counties in the PG file')

    return df


def compare_datasets(df1, df2):
    """
    Compares the two county data sets.

    Merges the dataframes so using an outer join. Adds merger indicator - either Original_only, PG_only, or both.
    Saves the merged dataframe to CSV file for inspection.

    :param df1: Original counties dataframe
    :type df1: pandas.DataFrame
    :param df2: PG counties dataframe
    :type df2: pandas.DataFrame

    :return: None
    """
    merged = df1.merge(df2, indicator=True, how='outer')

    both = merged[merged['_merge'] == 'both']

    right_msk = merged['_merge'] == 'right_only'
    right = merged[right_msk]
    left_msk = merged['_merge'] == 'left_only'
    left = merged[left_msk]

    print(merged['_merge'].cat.categories)
    merged['_merge'].cat.rename_categories(['Original_only', 'PG_only', 'both'], inplace=True)
    merged.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/information/counties_investigation.csv', index=False)

    print('Original Only:')
    print(left)

    print('PG:')
    print(right)

    print('both:')
    print(both)


if __name__ == '__main__':
    df1 = read_original_county_file()
    df2 = read_paul_county_file()
    compare_datasets(df1, df2)
