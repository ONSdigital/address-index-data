"""
ONS Address Index - Complete Address Parser
==================================

A simple script to check the logic for extracting SAO information from an input string.


Requirements
------------

:requires: pandas
:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: tqdm (4.10.0: https://github.com/tqdm/tqdm)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 28-Feb-2017
"""
import pandas as pd
from ProbabilisticParser import parser
from tqdm import tqdm


class AddressParser:
    """
    Address Parser class that implements the probabilistic parser and required pre- and post-processing steps.
    """

    @staticmethod
    def _normalize_input_data(data, normalised_field_name='ADDRESS_norm'):
        """
        Normalise input address information.

        This includes removal of commas and backslashes and whitespaces around numerical ranges.

        :param data: address data containing a column 'ADDRESS' to normalise
        :type data: pandas.DataFrame
        :param normalised_field_name: name of the new field to contain normalised address data
        :type normalised_field_name: str

        :return: normalised data containing a new column names as given by normalised_field_name
        :rtype: pandas.DataFrame
        """
        # make a copy of the actual address field and run the parsing against it
        data[normalised_field_name] = data['ADDRESS'].copy()

        # remove white spaces from the end and beginning if present
        data[normalised_field_name] = data[normalised_field_name].str.strip()

        # remove commas if present as not useful for matching
        data[normalised_field_name] = data[normalised_field_name].str.replace(', ', ' ')
        data[normalised_field_name] = data[normalised_field_name].str.replace(',', ' ')

        # remove backslash if present and replace with space
        data[normalised_field_name] = data[normalised_field_name].str.replace('\\', ' ')

        # remove spaces around hyphens as this causes ranges to be interpreted incorrectly
        # e.g. FLAT 15 191 - 193 NEWPORT ROAD CARDIFF CF24 1AJ is parsed incorrectly if there
        # is space around the hyphen
        data[normalised_field_name] = \
            data[normalised_field_name].str.replace(r'(\d+)(\s*-\s*)(\d+)', r'\1-\3', case=False)

        # some addresses have number TO number, while this should be with hyphen, replace TO with - in those cases
        # note: using \1 for group 1 and \3 for group 3 as I couldn't make non-capturing groups work
        data[normalised_field_name] = \
            data[normalised_field_name].str.replace(r'(\d+)(\s*TO\s*)(\d+)', r'\1-\3', case=False)

        # some addresses have number/number rather than - as the range separator
        data[normalised_field_name] = \
            data[normalised_field_name].str.replace(r'(\d+)(\s*/\s*)(\d+)', r'\1-\3', case=False)

        # some addresses have number+suffix - number+suffix, remove the potential whitespaces around the hyphen
        data[normalised_field_name] = \
            data[normalised_field_name].str.replace(r'(\d+[a-z])(\s*-\s*)(\d+[a-z])', r'\1-\3', case=False)

        return data

    @classmethod
    def parse(cls, data, normalised_field_name='ADDRESS_norm'):
        """
        Parse the address information given in the data.

        Assumes that the address information is stored in columned named 'ADDRESS'.

        :param data: address data containing a column 'ADDRESS' to parse
        :type data: pandas.DataFrame
        :param normalised_field_name: name of the new field to contain normalised address data
        :type normalised_field_name: str

        :return: parsed address data
        :rtype: pandas.DataFrame
        """
        data = cls._normalize_input_data(data, normalised_field_name=normalised_field_name)

        addresses = data[normalised_field_name].values

        # temp data storage lists
        organisation = []
        department = []
        sub_building = []
        building_name = []
        building_number = []
        street = []
        locality = []
        town = []
        postcode = []

        # loop over addresses and use the probabilistic parser to tag the address components - should avoid a loop
        for address in tqdm(addresses):
            parsed = parser.tag(address.upper())

            # store the parsed information to separate lists
            organisation.append(parsed.get('OrganisationName', None))
            department.append(parsed.get('DepartmentName', None))
            sub_building.append(parsed.get('SubBuildingName', None))
            building_name.append(parsed.get('BuildingName', None))
            building_number.append(parsed.get('BuildingNumber', None))
            street.append(parsed.get('StreetName', None))
            locality.append(parsed.get('Locality', None))
            town.append(parsed.get('TownName', None))
            postcode.append(parsed.get('Postcode', None))

        # add the parsed information to the dataframe
        data['OrganisationName'] = organisation
        data['DepartmentName'] = department
        data['SubBuildingName'] = sub_building
        data['BuildingName'] = building_name
        data['BuildingNumber'] = building_number
        data['StreetName'] = street
        data['Locality'] = locality
        data['TownName'] = town
        data['Postcode'] = postcode
        data['PAOText'] = data['BuildingName'].copy()
        data['SAOText'] = data['SubBuildingName'].copy()

        data = cls._parser_postprocessing(data)

        return data

    @staticmethod
    def _parser_postprocessing(data):
        """
        Parser post-processing steps.

        Extracts e.g. PAO_START, END, SAO_START, and END information from the parser tokens.

        :param data: parsed address data ready for post-processing
        :type data: pandas.DataFrame

        :return: parsed address data, which have gone through the post-processing steps
        :rtype: pandas.DataFrame
        """
        # if valid postcode information found then split between in and outcode
        if data['Postcode'].count() > 0:
            postcodes = data['Postcode'].str.split(' ', expand=True)
            postcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
            data = pd.concat([data, postcodes], axis=1)
        else:
            data['postcode_in'] = None
            data['postcode_out'] = None

        # data containers for those components not parsed, but derived during post-processing
        data['PAOstartNumber'] = None
        data['PAOendNumber'] = None
        data['PAOstartSuffix'] = None
        data['PAOendSuffix'] = None
        data['SAOStartNumber'] = None
        data['SAOEndNumber'] = None
        data['SAOStartSuffix'] = None
        data['SAOEndSuffix'] = None

        # if building number is present, then copy it to start number
        data['PAOstartNumber'] = data['BuildingNumber'].copy()

        # in some other cases / is in the BuildingName field - now this separates the building and flat
        # the first part refers to the building number and the second to the flat
        tmp = r'(\d+)\/(\d+)'
        msk = data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[1]

        # some cases the SAO components end up in the organisation name field, need to be separated
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z])'
        msk = data['OrganisationName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'OrganisationName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[1]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[2]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[3]

        # some cases the SAO components end up in the organisation name field, need to be separated
        tmp = r'(\d+)-(\d+)([A-Z])'
        msk = data['OrganisationName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'OrganisationName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[1]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[2]

        # sometimes both PAO and SAO range is in the BuildingName e.g. "35A-35D 35A-35F"
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z]).*?(\d+)([A-Z])-(\d+)([A-Z])'
        msk = data['BuildingNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[1]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[2]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[3]
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[4]
        data.loc[msk & data['PAOstartSuffix'].isnull(), 'PAOstartSuffix'] = extracted_components[5]
        data.loc[msk & data['PAOendNumber'].isnull(), 'PAOendNumber'] = extracted_components[6]
        data.loc[msk & data['PAOendSuffix'].isnull(), 'PAOendSuffix'] = extracted_components[7]

        # sometimes both PAO and SAO range is in the BuildingName e.g. "28A-28F PICCADILLY COURT 457-463"
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z]).*?(\d+)-(\d+)'
        msk = data['BuildingNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[1]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[2]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[3]
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[4]
        data.loc[msk & data['PAOendNumber'].isnull(), 'PAOendNumber'] = extracted_components[5]

        # sometimes both PAO and SAO range is in the BuildingName e.g. "3-3A CHURCHILL COURT 112-144"
        tmp = r'(\d+)-(\d+)([A-Z]).*?(\d+)-(\d+)'
        msk = data['BuildingNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[1]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[2]
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[3]
        data.loc[msk & data['PAOendNumber'].isnull(), 'PAOendNumber'] = extracted_components[4]

        # sometimes both building number and flat range are stored in BuildingName (e.g. 9B-9C 65A), separate these
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z])\s.*?(\d+)([A-Z])'
        msk = data['BuildingNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[1]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[2]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[3]
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[4]
        data.loc[msk & data['PAOstartSuffix'].isnull(), 'PAOstartSuffix'] = extracted_components[5]

        # if building number is not present, try to extract from building name if appropriate type
        # deal with cases where buildingName contains a suffix range: 24D-24E
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z])'
        msk = data['PAOstartNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[0]
        data.loc[msk & data['PAOstartSuffix'].isnull(), 'PAOstartSuffix'] = extracted_components[1]
        data.loc[msk & data['PAOendNumber'].isnull(), 'PAOendNumber'] = extracted_components[2]
        data.loc[msk & data['PAOendSuffix'].isnull(), 'PAOendSuffix'] = extracted_components[3]
        # deal with cases where buildingName contains a suffix range: 24-24E
        tmp = r'(\d+)-(\d+)([A-Z])'
        msk = data['PAOstartNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[0]
        data.loc[msk & data['PAOendNumber'].isnull(), 'PAOendNumber'] = extracted_components[1]
        data.loc[msk & data['PAOendSuffix'].isnull(), 'PAOendSuffix'] = extracted_components[2]
        # deal with cases where buildingName is a range: 120-122
        tmp = r'(\d+)-(\d+)'
        msk = data['PAOstartNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[0]
        data.loc[msk & data['PAOendNumber'].isnull(), 'PAOendNumber'] = extracted_components[1]
        # deal with cases where buildingName is 54A or 65B but not part of a range e.g. 65A-65B
        tmp = r'(?<!-|\d)(\d+)([A-Z])(?!-)'
        msk = data['PAOstartNumber'].isnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['PAOstartNumber'].isnull(), 'PAOstartNumber'] = extracted_components[0]
        data.loc[msk & data['PAOstartSuffix'].isnull(), 'PAOstartSuffix'] = extracted_components[1]

        # if building start number is present, then add to SAO
        # sometimes subBuildingName contains the flat range e.g. 14E-14E extract the components
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z])'
        msk = data['SubBuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'SubBuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[1]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[2]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[3]

        # sometimes subBuildingName contains the flat range e.g. 14-14E extract the components
        tmp = r'(\d+)-(\d+)([A-Z])'
        msk = data['SubBuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'SubBuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[1]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[2]

        # sometimes subBuildingName is e.g. C2 where to number refers to the flat number
        tmp = r'([A-Z])(\d+)'
        msk = data['SubBuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'SubBuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[1]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[0]

        # deal with cases where buildingName contains a suffix range: 24D-24E
        tmp = r'(\d+)([A-Z])-(\d+)([A-Z])'
        msk = data['PAOstartNumber'].notnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOStartSuffix'].isnull(), 'SAOStartSuffix'] = extracted_components[1]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[2]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[3]

        # deal with cases where buildingName contains a suffix range: 24-24E
        tmp = r'(\d+)-(\d+)([A-Z])'
        msk = data['PAOstartNumber'].notnull() & data['BuildingName'].str.contains(tmp, na=False, case=False)
        extracted_components = data.loc[msk, 'BuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SAOStartNumber'] = extracted_components[0]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SAOEndNumber'] = extracted_components[1]
        data.loc[msk & data['SAOEndSuffix'].isnull(), 'SAOEndSuffix'] = extracted_components[2]

        # some addresses have / as the separator for buildings and flats, when matching against NLP, needs "FLAT"
        msk = data['SubBuildingName'].str.contains('\d+\/\d+', na=False, case=False)
        data.loc[msk, 'SubBuildingName'] = 'FLAT ' + data.loc[msk, 'SubBuildingName']

        # deal with addresses that are of type 5/7 4 whatever road, the format assumed start/end_sao_number pao_start_numb
        tmp = r'(\d+)\/(\d+)'
        msk = data['SubBuildingName'].str.contains(tmp, na=False, case=False) & \
              data['SAOStartNumber'].isnull() & data['BuildingNumber'].notnull()
        extracted_components = data.loc[msk, 'SubBuildingName'].str.extract(tmp)
        data.loc[msk & data['SAOStartNumber'].isnull(), 'SubBuildingName'] = extracted_components[0]
        data.loc[msk & data['SAOEndNumber'].isnull(), 'SubBuildingName'] = extracted_components[1]

        # if SubBuildingName contains only numbers, then place also to the sao start number field as likely to be flat
        msk = data['SubBuildingName'].str.isnumeric() & data['SAOStartNumber'].isnull()
        msk[msk.isnull()] = False
        data.loc[msk, 'SAOStartNumber'] = data.loc[msk, 'SubBuildingName']

        return data
