"""
GeoPlace does not do address parsing, so attach original
parsed addresses to the comparison files.
"""
import pandas as pd


def _join(minimum, original):
    """

    :param minimum:
    :param original:
    :return:
    """
    data = pd.merge(minimum, original, how='left', on='ID')
    return data


def attach_original_to_CH():
    """

    :return:
    """
    filename_minimum = '/Users/saminiemi/Dropbox/ONS/CompaniesHouse_150k.csv'
    filename_original = '/Users/saminiemi/Projects/ONS/AddressIndex/data/BasicCompanyData-2016-12-01-part1_5.csv'

    minimum_data = pd.read_csv(filename_minimum, low_memory=False)

    usecols = ['CompanyName', 'CompanyNumber', 'RegAddress.AddressLine1', 'RegAddress.AddressLine2',
               'RegAddress.PostTown', 'RegAddress.County', 'RegAddress.PostCode']
    original_data = pd.read_csv(filename_original, usecols=usecols, low_memory=False)
    original_data.rename(columns={'CompanyNumber': 'ID'}, inplace=True)

    data = _join(minimum_data, original_data)
    data.to_csv('/Users/saminiemi/Dropbox/ONS/CompaniesHouse_150k_modified.csv', index=False)


def attach_original_to_LR():
    """

    :return:
    """
    filename_minimum = '/Users/saminiemi/Dropbox/ONS/LandRegistry_minimal.csv'
    filename_original = '/Users/saminiemi/Projects/ONS/AddressIndex/data/pp-monthly-update-Edited.csv'

    minimum_data = pd.read_csv(filename_minimum, low_memory=False)

    original_data = pd.read_csv(filename_original, low_memory=False)
    original_data.rename(columns={'TransactionID': 'ID'}, inplace=True)

    data = _join(minimum_data, original_data)

    data.drop(['Price', 'TransferDate', 'Type', 'New', 'Duration', 'PPD', 'RecordStatus'], axis=1, inplace=True)
    data.to_csv('/Users/saminiemi/Dropbox/ONS/LandRegistry_minimal_modified.csv', index=False)


if __name__ == "__main__":
    attach_original_to_LR()

    attach_original_to_CH()
