"""
ONS Address Index - Linking Prototype
=====================================

Contains a class, which implements an Address Linking Prototype (ALP).

This is a prototype code aimed for experimentation and testing. There are not unit tests.
The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.



Requirements
------------

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: pandas ( 0.19.1)
:requires: numpy (1.11.2)
:requires: tqdm (4.10.0: https://github.com/tqdm/tqdm)
:requires: recordlinkage (0.7.2: https://pypi.python.org/pypi/recordlinkage/)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 28-Nov-2016
"""
import datetime
import re
import time
import os
import warnings
import logger
import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import recordlinkage
from ProbabilisticParser import parser
from tqdm import tqdm

warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None


class Linker(object):
    """

    :param verbose: whether or not output information
    :type verbose: bool
    :param test: whether or not to use test data
    :type test: bool
    :param expandSynonyms: whether to expand common synonyms or not
    :type expandSynonyms: bool
    :param ABpath: location of the AddressBase combined data file
    :type ABpath: str
    :param ABfilename: name of the file containing modified AddressBase
    :type ABfilename: str
    :param inputFilename: name of the CSV file holding the data
    :type inputFilename: str
    :param inputPath: location of the test data
    :type inputPath: str

    """

    def __init__(self, **kwargs):
        """

        :param kwargs:
        :type kwargs: dict
        """
        # set up and update settings
        self.settings = dict(inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                             inputFilename='WelshGovernmentData21Nov2016.csv',
                             ABpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                             ABfilename='AB.csv',
                             log='ALP',
                             dropColumns=False,
                             expandSynonyms=True,
                             test=False,
                             verbose=False)
        self.settings.update(kwargs)

        # relative path when referring to data files
        self.currentDirectory = os.path.dirname(__file__)  # for relative path definitions

        # define containers
        self.nExistingUPRN = 0
        self.toLinkAddressData = None

        # set up a logger, use date and time as filename
        start_date = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.log = logger.set_up_logger(self.settings['log'] + start_date + '.log')
        self.log.info('A new Linking Run Started with the following settings')
        self.log.info(self.settings)

        # read in AddressBase
        start = time.clock()
        self._load_addressbase()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

    def load_data(self):
        """
        Read in the data that need to be linked.
        """
        if self.settings['test']:
            self.log.info('Reading in test data...')
            self.settings['inputFilename'] = 'testData.csv'
        else:
            self.log.info('Reading in data that need to be linked...')

        self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                             low_memory=False)

        # fill NaNs with empty strings so that we can form a single address string
        self.toLinkAddressData.fillna('', inplace=True)
        self.toLinkAddressData['ADDRESS'] = self.toLinkAddressData['Building'] + ' ' + \
                                            self.toLinkAddressData['Street'] + ' ' + \
                                            self.toLinkAddressData['Locality'] + ' ' + \
                                            self.toLinkAddressData['Town'] + ' ' + \
                                            self.toLinkAddressData['County'] + ' ' + \
                                            self.toLinkAddressData['Postcode']

        # rename postcode to postcode_orig and locality to locality_orig
        self.toLinkAddressData.rename(columns={'UPRNs_matched_to_date': 'UPRN_prev'}, inplace=True)

        # convert original UPRN to numeric
        self.toLinkAddressData['UPRN_prev'] = self.toLinkAddressData['UPRN_prev'].convert_objects(convert_numeric=True)

        if self.settings['verbose']:
            self.log.info(self.toLinkAddressData.info())

        self.log.info('Found {} addresses...'.format(len(self.toLinkAddressData.index)))
        self.nExistingUPRN = len(self.toLinkAddressData.loc[self.toLinkAddressData['UPRN_prev'].notnull()].index)
        self.log.info('{} with UPRN already attached...'.format(self.nExistingUPRN))

        # set index name - needed later for merging / duplicate removal
        self.toLinkAddressData.index.name = 'TestData_Index'

    def _load_addressbase(self):
        """
        Load a compressed version of the full AddressBase file. The information being used
        has been processed from a AB Epoch 39 files provided by ONS.

        .. Note: this function modifies the original AB information by e.g. combining different tables. Such
                 activities are undertaken because of the aggressive blocking the prototype linking code uses.
                 The actual production system should take AB as it is and the linking should not perform blocking
                 but rather be flexible and take into account that in NAG the information can be stored in various
                 fields.
        """
        if self.settings['test']:
            self.log.info('Reading in Address Base Test Data...')
            self.settings['ABfilename'] = 'ABtest.csv'
        else:
            self.log.info('Reading in Address Base Data...')

        self.addressBase = pd.read_csv(self.settings['ABpath'] + self.settings['ABfilename'],
                                       dtype={'UPRN': np.int64, 'POSTCODE_LOCATOR': str, 'ORGANISATION_NAME': str,
                                              'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                                              'BUILDING_NUMBER': str, 'THROUGHFARE': str, 'DEPENDENT_LOCALITY': str,
                                              'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                                              'PAO_START_NUMBER': str, 'PAO_START_SUFFIX': str, 'PAO_END_NUMBER': str,
                                              'PAO_END_SUFFIX': str, 'SAO_TEXT': str, 'SAO_START_NUMBER': str,
                                              'SAO_START_SUFFIX': str, 'ORGANISATION': str, 'STREET_DESCRIPTOR': str,
                                              'TOWN_NAME': str, 'LOCALITY': str})
        self.log.info('Found {} addresses from AddressBase...'.format(len(self.addressBase.index)))

        # combine information - could be done differently, but for now using some of these for blocking
        msk = self.addressBase['THROUGHFARE'].isnull()
        self.addressBase.loc[msk, 'THROUGHFARE'] = self.addressBase.loc[msk, 'STREET_DESCRIPTOR']

        msk = self.addressBase['ORGANISATION_NAME'].isnull()
        self.addressBase.loc[msk, 'ORGANISATION_NAME'] = self.addressBase.loc[msk, 'ORGANISATION']

        msk = self.addressBase['POSTCODE'].isnull()
        self.addressBase.loc[msk, 'POSTCODE'] = self.addressBase.loc[msk, 'POSTCODE_LOCATOR']

        msk = self.addressBase['SUB_BUILDING_NAME'].isnull()
        self.addressBase.loc[msk, 'SUB_BUILDING_NAME'] = self.addressBase.loc[msk, 'SAO_TEXT']

        msk = self.addressBase['POST_TOWN'].isnull()
        self.addressBase.loc[msk, 'POST_TOWN'] = self.addressBase.loc[msk, 'TOWN_NAME']

        msk = self.addressBase['LOCALITY'].isnull()
        self.addressBase.loc[msk, 'LOCALITY'] = self.addressBase.loc[msk, 'DEPENDENT_LOCALITY']

        # drop some that are not needed
        self.addressBase.drop(['DEPENDENT_LOCALITY', 'POSTCODE_LOCATOR'], axis=1, inplace=True)

        # split postcode to in and outcode, useful in different ways of performing blocking
        pcodes = self.addressBase['POSTCODE'].str.split(' ', expand=True)
        pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
        self.addressBase = pd.concat([self.addressBase, pcodes], axis=1)

        # rename some columns (sorted windowing requires column names to match)
        self.addressBase.rename(columns={'THROUGHFARE': 'StreetName',
                                         'POST_TOWN': 'townName',
                                         'POSTCODE': 'postcode',
                                         'PAO_TEXT': 'pao_text',
                                         'LOCALITY': 'locality',
                                         'BUILDING_NAME': 'BuildingName'}, inplace=True)

        # # # if SubBuildingName is empty add dummy - helps as string distance cannot be computed between Nones
        msk = self.addressBase['SUB_BUILDING_NAME'].isnull()
        self.addressBase.loc[msk, 'SUB_BUILDING_NAME'] = 'N/A'

        # set index name - needed later for merging / duplicate removal
        self.addressBase.index.name = 'AddressBase_Index'

    @staticmethod
    def _extract_postcode(string):
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

    def _normalize_input_data(self):
        """
        Normalize the address information. Removes white spaces, commas, and backslashes.
        Can be used to expand common synonyms such as RD or BERKS. Finally parses counties
        as the an early version of the probabilistic parser was not trained to parser counties.
        """
        # make a copy of the actual address field and run the parsing against it
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData['ADDRESS'].copy()

        # remove white spaces if present
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData['ADDRESS_norm'].str.strip()

        # remove commas and apostrophes and insert space
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData.apply(lambda x:
                                                                              x['ADDRESS_norm'].replace(',', ' '),
                                                                              axis=1)

        # remove backslash if present and replace with space
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData.apply(lambda x:
                                                                              x['ADDRESS_norm'].replace('\\', ' '),
                                                                              axis=1)

        # remove spaces around hyphens as this causes ranges to be interpreted incorrectly
        # e.g. FLAT 15 191 - 193 NEWPORT ROAD  CARDIFF CF24 1AJ is parsed incorrectly if there
        # is space around the hyphen
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData.apply(lambda x:
                                                                              x['ADDRESS_norm'].replace(' - ', '-'),
                                                                              axis=1)

        # synonyms to expand - read from a file with format (from, to)
        synonyms = pd.read_csv(os.path.join(self.currentDirectory, '../../data/') + 'synonyms.csv').values

        # expand common synonyms to help with parsing
        if self.settings['expandSynonyms']:
            self.log.info('Expanding synonyms as a part of normalisation...')
            for fro, to in synonyms:
                self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData['ADDRESS_norm'].str.replace(fro, to)

        # parsing gets really confused if region or county is in the line - get known counties from a file
        counties = pd.read_csv(os.path.join(self.currentDirectory, '../../data/') + 'counties.csv')['county']

        # use this for the counties so that e.g. ESSEX ROAD does not become just ROAD...
        # todo: the regex is getting ridiculous, maybe do other way around i.e. country must be followed by postcode or
        #       be the last component.
        addRegex = '(?:\s)(?!ROAD|LANE|STREET|CLOSE|DRIVE|AVENUE|SQUARE|COURT|PARK|CRESCENT|WAY|WALK|HEOL|FFORDD|HILL|GARDENS|GATE|GROVE|HOUSE|VIEW|BUILDING|VILLAS|LODGE|PLACE|ROW|WHARF|RISE|TERRACE|CROSS|ENTERPRISE|HATCH)'

        # remove county from address but add a column for it
        self.toLinkAddressData['County'] = None
        for county in counties:
            msk = self.toLinkAddressData['ADDRESS_norm'].str.contains(county + addRegex, regex=True, na=False)
            self.toLinkAddressData.loc[msk, 'County'] = county
            self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData['ADDRESS_norm'].str.replace(county +
                                                                                                        addRegex, '',
                                                                                                        case=False)

    def _fix_london_boroughs(self, parsed):
        """
        A method to address incorrectly parsed London boroughs.
        If the street name contains London borough then move it to locality and remove from the street name.

        :param parsed: a dictionary containing the address tokens that have been parsed
        :type parsed: dict

        :return:
        """
        LondonLocalities = pd.read_csv(os.path.join(self.currentDirectory, '../../data/') + 'localities.csv')['locality']

        for LondonLocality in LondonLocalities:
            if parsed['StreetName'].strip().endswith(LondonLocality):
                parsed['LondonLocalityality'] = LondonLocality
                # take the last part out, so that e.g. CHINGFORD AVENUE CHINGFORD is correctly processed
                # need to be careful with e.g.  WESTERN GATEWAY ROYAL VICTORIA DOCK (3 parts to remove)
                parsed['StreetName'] = parsed['StreetName'].strip()[:-len(LondonLocality)].strip()

        return parsed

    def parse_input_addresses_to_tokens(self):
        """
        Parses the address information from the input data.
        """
        # normalise data so that the parser has the best possible chance of getting things right
        self._normalize_input_data()

        # get addresses and store separately as an vector
        addresses = self.toLinkAddressData['ADDRESS_norm'].values
        self.log.info('Parsing {} addresses...'.format(len(addresses)))

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
            parsed = parser.tag(address.upper())  # probabilistic parser
            pcode = self._extract_postcode(address)  # regular expression extraction

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
                    parsed = self._fix_london_boroughs(parsed)

            # if BuildingName is e.g. 55A then should get the number and suffix separately
            if parsed.get('BuildingName', None) is not None:
                parsed['BuildingSuffix'] = ''.join([x for x in parsed['BuildingName'] if not x.isdigit()])
                # accept suffixes that are only maximum two chars and if not hyphen
                if len(parsed['BuildingSuffix']) > 2 and (parsed['BuildingSuffix'] != '-'):
                    parsed['BuildingSuffix'] = None
                    # todo: if the identified suffix is hyphen, then actually a number range and should separate start from stop

            # some addresses contain place CO place, where the CO is not part of the actual name - remove these
            # same is true for IN e.g. Road Marton IN Cleveland
            if parsed.get('Locality', None) is not None:
                if parsed['Locality'].strip().endswith(' CO'):
                    parsed['Locality'] = parsed['Locality'].replace(' CO', '')
                if parsed['Locality'].strip().endswith(' IN'):
                    parsed['Locality'] = parsed['Locality'].replace(' IN', '')

            # sometimes building number gets placed at building name, take it and add to building name
            if parsed.get('BuildingNumber', None) is None and parsed.get('BuildingName', None) is not None:
                tmp = parsed['BuildingName'].split(' ')
                if len(tmp) > 1:
                    try:
                        _ = int(tmp[0])
                        parsed['BuildingNumber'] = tmp[0]
                    except ValueError:
                        pass

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
        self.toLinkAddressData['OrganisationName'] = organisation
        self.toLinkAddressData['DepartmentName'] = department
        self.toLinkAddressData['SubBuildingName'] = subbuilding
        self.toLinkAddressData['BuildingName'] = buildingname
        self.toLinkAddressData['BuildingNumber'] = buildingnumber
        self.toLinkAddressData['StreetName'] = street
        self.toLinkAddressData['Locality'] = locality
        self.toLinkAddressData['TownName'] = town
        self.toLinkAddressData['Postcode'] = postcode
        self.toLinkAddressData['BuildingSuffix'] = buildingsuffix

        # if valid postcode information found then split between in and outcode
        if self.toLinkAddressData['Postcode'].count() > 0:
            pcodes = self.toLinkAddressData['Postcode'].str.split(' ', expand=True)
            pcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
            self.toLinkAddressData = pd.concat([self.toLinkAddressData, pcodes], axis=1)
        else:
            self.toLinkAddressData['postcode_in'] = None
            self.toLinkAddressData['postcode_out'] = None

        # # split flat or apartment number as separate for numerical comparison - compare e.g. SAO number
        self.toLinkAddressData['FlatNumber'] = None
        msk = self.toLinkAddressData['SubBuildingName'].str.contains('flat|apartment', na=False, case=False)
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = self.toLinkAddressData.loc[msk, 'SubBuildingName']
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = \
            self.toLinkAddressData.loc[msk].apply(lambda x: x['FlatNumber'].strip().
                                                  replace('FLAT', '').replace('APARTMENT', ''), axis=1)
        self.toLinkAddressData['FlatNumber'] = pd.to_numeric(self.toLinkAddressData['FlatNumber'], errors='coerce')

        # if SubBuilding name or organisation name is empty add dummy
        msk = self.toLinkAddressData['SubBuildingName'].isnull()
        self.toLinkAddressData.loc[msk, 'SubBuildingName'] = 'N/A'

        # fill columns that are often NA with empty strings - helps when doing string comparisons against Nones
        columnsToAddEmptyStrings = ['OrganisationName', 'DepartmentName', 'SubBuildingName', 'BuildingSuffix']
        self.toLinkAddressData[columnsToAddEmptyStrings].fillna('', inplace=True)

        # save for inspection
        self.toLinkAddressData.to_csv('/Users/saminiemi/Projects/ONS/AddressIndex/data/ParsedAddresses.csv',
                                      index=False)

        # drop the temp info
        self.toLinkAddressData.drop(['ADDRESS_norm', ], axis=1, inplace=True)

    def link_addresses_with_postcode(self, toMatch, limit=0.1, buildingNumberBlocking=True):
        """
        Link toMatch data to the AddressBase source information.

        Uses blocking to speed up the matching. This is dangerous for postcodes that have been misspelled
        and will potentially lead to false positives.

        .. note: the aggressive blocking does not work when both BuildingNumber and BuildingName is missing.
                 This is somewhat common for example for care homes. One should really separate these into
                 different category and do blocking only on postcode.

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
        pcl = recordlinkage.Pairs(toMatch, self.addressBase)

        # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
        # block on both postcode and house number, street name can have typos and therefore is not great for blocking
        if buildingNumberBlocking:
            self.log.info(
                'Start matching those with postcode information, using postcode and building number blocking...')
            pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'], right_on=['postcode', 'BUILDING_NUMBER'])
        else:
            self.log.info('Start matching those with postcode information, using postcode blocking...')
            pairs = pcl.block(left_on=['Postcode'], right_on=['postcode'])

        self.log.info('Need to test {0} pairs for {1} addresses...'.format(len(pairs), len(toMatch.index)))

        # compare the two data sets
        # the idea is to build evidence to support linking, hence some fields are compared multiple times
        compare = recordlinkage.Compare(pairs, self.addressBase, toMatch, batch=True)

        # set rules for standard residential addresses
        compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl', missing_value=0.6)
        compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl', missing_value=0.6)
        compare.string('BuildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl',
                       missing_value=0.5)
        compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl',
                       missing_value=0.5)
        compare.string('StreetName', 'StreetName', method='damerau_levenshtein', name='street_dl')
        compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl')
        compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl', missing_value=0.5)

        # use to separate e.g. 55A from 55
        compare.string('PAO_START_SUFFIX', 'BuildingSuffix', method='damerau_levenshtein', name='pao_suffix_dl',
                       missing_value=0.5)

        # the following is good for flats and apartments than have been numbered
        compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='damerau_levenshtein', name='flatw_dl',
                       missing_value=0.5)
        compare.string('SAO_START_NUMBER', 'FlatNumber', method='damerau_levenshtein', name='sao_number_dl',
                       missing_value=0.6)
        # some times the PAO_START_NUMBER is 1 for the whole house without a number and SAO START NUMBER refers
        # to the flat number, but the flat number is actually part of the house number without flat/apt etc. specifier
        # This comparison should probably be numeric.
        compare.string('SAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='sao_number2_dl')

        # set rules for organisations such as care homes and similar type addresses
        compare.string('ORGANISATION', 'OrganisationName', method='damerau_levenshtein', name='organisation_dl',
                       missing_value=0.5)

        # execute the comparison model
        compare.run()

        # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
        compare.vectors['pao_dl'] *= 5.
        compare.vectors['sao_number_dl'] *= 4.
        compare.vectors['flat_dl'] *= 3.
        compare.vectors['building_name_dl'] *= 3.
        compare.vectors['pao_suffix_dl'] *= 2.

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
        matches = matches.drop_duplicates('TestData_Index', keep='first')

        # sort by WG_Index
        matches = matches.sort_values(by='TestData_Index')

        # matched IDs
        matchedIndex = matches['TestData_Index'].values

        # missing ones
        missingIndex = toMatch.index.difference(matchedIndex)
        missing = toMatch.loc[missingIndex]

        self.log.info('Found {} matches...'.format(len(matches.index)))
        self.log.info('Failed to found {} matches...'.format(len(missing.index)))

        return matches, missing

    def link_addresses_without_postcode(self, toMatch, limit=0.7, buildingNumberBlocking=True):
        """
        Link toMatch data to the AddressBase source information.
        Uses blocking to speed up the matching.

        .. note: the aggressive blocking that uses street name rules out any addresses with a typo in the street name.
                 Given that this is fairly common, one should use sorted neighbourhood search instead of blocking.
                 However, given that this prototype needs to run on a laptop with limited memory in a reasonable time
                 the aggressive blocking is used. In production, no blocking should be used.

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
        pcl = recordlinkage.Pairs(toMatch, self.addressBase)

        # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
        if buildingNumberBlocking:
            self.log.info(
                'Start matching those without postcode information, using building number and street name blocking...')
            pairs = pcl.block(left_on=['BuildingNumber', 'StreetName'], right_on=['BUILDING_NUMBER', 'StreetName'])
        else:
            self.log.info(
                'Start matching those without postcode information, using building name and street name blocking...')
            pairs = pcl.block(left_on=['BuildingName', 'StreetName'], right_on=['BuildingName', 'StreetName'])

        self.log.info('Need to test {0} pairs for {1} addresses...'.format(len(pairs), len(toMatch.index)))

        # compare the two data sets - use different metrics for the comparison
        # the idea is to build evidence to support linking, hence some fields are compared multiple times
        compare = recordlinkage.Compare(pairs, self.addressBase, toMatch, batch=True)

        compare.string('SAO_START_NUMBER', 'FlatNumber', method='damerau_levenshtein', name='sao_number_dl')
        compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl')
        if buildingNumberBlocking:
            compare.string('BuildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl')
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
        matches = matches.drop_duplicates('TestData_Index', keep='first')

        # sort by WG_Index
        matches = matches.sort_values(by='TestData_Index')

        self.log.info('Found {} matches...'.format(len(matches.index)))

        return matches

    def merge_linked_data_and_address_base_information(self, matches):
        """
        Merge address base information to the identified matches.
        Outputs the merged information to a CSV file for later inspection.

        :param matches: found matches
        :type matches: pandas.DataFrame

        :return: merged dataframe
        :rtype: pandas.DataFrame
        """
        # merge to original information to the matched data
        self.toLinkAddressData = self.toLinkAddressData.reset_index()
        data = pd.merge(matches, self.toLinkAddressData, how='left', on='TestData_Index')
        data = pd.merge(data, self.addressBase, how='left', on='AddressBase_Index')

        # drop unnecessary columns
        if self.settings['dropColumns']:
            data.drop(['TestData_Index', 'AddressBase_Index'], axis=1, inplace=True)

        return data

    def confirm_results(self, df):
        """

        :param df:
        :return:
        """
        # find those without match
        IDs = df['ID'].values
        missing_msk = ~self.toLinkAddressData['ID'].isin(IDs)
        missing = self.toLinkAddressData.loc[missing_msk]
        self.log.info('{} addresses were not linked...'.format(len(missing.index)))

        nOldUPRNs = len(df.loc[df['UPRN_prev'].notnull()].index)
        self.log.info('{} previous UPRNs in the matched data...'.format(nOldUPRNs))

        # find those with UPRN attached earlier and check which are the same
        msk = df['UPRN_prev'] == df['UPRN']
        matches = df.loc[msk]
        self.log.info('{} addresses have the same UPRN as earlier...'.format(len(matches.index)))
        pdt.assert_series_equal(df['UPRN_prev'], df['UPRN'])


    def check_performance(self, df,
                          prefix='WelshGov', path='/Users/saminiemi/Projects/ONS/AddressIndex/data/'):
        """
        Check performance - calculate the match rate.

        :param df: data frame with linked addresses and similarity metrics
        :type df: pandas.DataFrame
        :param prefix: prefix name for the output files
        :type prefix: str
        :param path: location where to store the output files
        :type path: str

        :return: None
        """
        # count the number of matches and number of edge cases
        nmatched = len(df.index)
        total = len(self.toLinkAddressData.index)

        # how many were matched
        self.log.info('Matched {} entries'.format(nmatched))
        self.log.info('Total Match Fraction {} per cent'.format(round(nmatched / total * 100., 1)))

        # save matched
        df.to_csv(path + prefix + '_matched.csv', index=False)

        # find those without match
        IDs = df['ID'].values
        missing_msk = ~self.toLinkAddressData['ID'].isin(IDs)
        missing = self.toLinkAddressData.loc[missing_msk]
        missing.to_csv(path + prefix + '_matched_missing.csv', index=False)
        self.log.info('{} addresses were not linked...'.format(len(missing.index)))

        nOldUPRNs = len(df.loc[df['UPRN_prev'].notnull()].index)
        self.log.info('{} previous UPRNs in the matched data...'.format(nOldUPRNs))

        # find those with UPRN attached earlier and check which are the same
        msk = df['UPRN_prev'] == df['UPRN']
        matches = df.loc[msk]
        matches.to_csv(path + prefix + '_sameUPRN.csv', index=False)
        self.log.info('{} addresses have the same UPRN as earlier...'.format(len(matches.index)))

        # find those that has a previous UPRN but does not mach a new one (filter out nulls)
        msk = df['UPRN_prev'].notnull()
        notnulls = df.loc[msk]
        nonmatches = notnulls.loc[notnulls['UPRN_prev'] != notnulls['UPRN']]
        nonmatches.to_csv(path + prefix + '_differentUPRN.csv', index=False)
        self.log.info('{} addresses have a different UPRN as earlier...'.format(len(nonmatches.index)))

        # find all newly linked
        newUPRNs = df.loc[~msk]
        newUPRNs.to_csv(path + prefix + '_newUPRN.csv', index=False)
        self.log.info('{} more addresses with UPRN...'.format(len(newUPRNs.index)))

    def run_test(self):
        """
        Run all required steps.

        :return: None
        """

        start = time.clock()
        self.load_data()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        self.log.info('Parsing address data...')
        start = time.clock()
        self.parse_input_addresses_to_tokens()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        # split to those with full postcode and no postcode - use different matching strategies
        msk = self.toLinkAddressData['Postcode'].isnull()
        withPC = self.toLinkAddressData.loc[~msk]
        noPC = self.toLinkAddressData.loc[msk]

        self.log.info('Matching addresses against Address Base data...')
        start = time.clock()
        ms1 = ms2 = m1a = m1b = m2a = m2b = False
        if len(withPC.index) > 0:
            msk = withPC['BuildingNumber'].isnull()
            withPCnoHouseNumber = withPC.loc[msk]
            withPCHouseNumber = withPC.loc[~msk]
            if len(withPCHouseNumber) > 0:
                matches1a, missing1a = self.link_addresses_with_postcode(withPCHouseNumber, buildingNumberBlocking=True)
                m1a = True
            if len(withPCnoHouseNumber) > 0:
                if len(missing1a.index) > 0:
                    # todo: fix this - should test that missing1a actually exists (complete rewrite needed when refactoring)
                    withPCnoHouseNumber = withPCnoHouseNumber.append(missing1a)
                matches1b, missing1b = self.link_addresses_with_postcode(withPCnoHouseNumber, buildingNumberBlocking=False)
                m1b = True
            ms1 = True
        if len(noPC.index) > 0:
            msk = noPC['BuildingNumber'].isnull()
            noPCnoHouseNumber = noPC.loc[msk]
            noPCHouseNumber = noPC.loc[~msk]
            if len(noPCnoHouseNumber) > 0:
                matches2a = self.link_addresses_without_postcode(noPCHouseNumber, buildingNumberBlocking=True)
                m2a = True
            if len(noPCHouseNumber) > 0:
                matches2b = self.link_addresses_without_postcode(noPCnoHouseNumber, buildingNumberBlocking=False)
                m2b = True
            ms2 = True
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        self.log.info('Merging back the original information...')
        start = time.clock()
        self.addressBase = self.addressBase.reset_index()

        # most horrifying code ever... todo: complete rewrite required
        m1 = m2 = False
        if ms1:
            if m1a:
                matched1a = self.merge_linked_data_and_address_base_information(matches1a)
            if m1b:
                matched1b = self.merge_linked_data_and_address_base_information(matches1b)
            m1 = True
        if ms2:
            if m2a:
                matched2a = self.merge_linked_data_and_address_base_information(matches2a)
            if m2b:
                matched2b = self.merge_linked_data_and_address_base_information(matches2b)
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
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        if self.settings['test']:
            self.confirm_results(matched)
        else:
            self.log.info('Checking Performance...')
            self.check_performance(matched)

        print('Finished running')


if __name__ == "__main__":
    linker = Linker(**dict(test=True))
    linker.run_test()
