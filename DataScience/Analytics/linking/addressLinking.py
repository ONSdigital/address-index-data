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
:requires: recordlinkage (0.6.0: https://pypi.python.org/pypi/recordlinkage/)
:requires: matplotlib (1.5.3)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 30-Nov-2016
"""
import datetime
import re
import time
import os
import warnings
import sqlite3
import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import matplotlib.pyplot as plt
import recordlinkage as rl
from Analytics.linking import logger
from ProbabilisticParser import parser
from tqdm import tqdm

warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None

__version__ = '0.1'


class AddressLinker:
    """
    This class forms the backbone of the Address Linking prototype developed for ONS
    as a part of the Address Index project.

    The class implements methods to read in AddressBase, to normalise and parse address strings,
    link input data against AddressBase, and finally to merge the test data with the AddressBase
    information. It should be noted that the load_data method should be overwritten and made
    appropriate for each input test file which maybe be in different formats. In addition, the
    check_performance method should also be overwritten because some datasets may or may not
    contain already attached UPRNs and different confidences may have been attached to these UPRNs.
    """

    def __init__(self, **kwargs):
        """
        Class constructor.

        :param kwargs: arguments to control the program flow and set paths and filenames.
        :type kwargs: dict

        :Keyword Arguments:
            * :param inputPath: location of the test data
            * :type inputPath: str
            * :param inputFilename: name of the CSV file holding the data
            * :type inputFilename: str
            * :param ABpath: location of the AddressBase combined data file
            * :type ABpath: str
            * :param ABfilename: name of the file containing modified AddressBase
            * :type ABfilename: str
            * :param log: name of the log file
            * :type log: str
            * :param limit: the sum of the matching metrics need to be above this limit to count as a potential match.
                          Affects for example the false positive rate.
            * :type limit: float
            * :param outname: a string that is prepended to the output data
            * :type outname: str
            * :param outpath: location to which to store the output data
            * :type outpath: str
            * :param dropColumns: whether or not to drop extra columns that are created during the linking
            * :type dropColumns: bool
            * :param expandSynonyms: whether to expand common synonyms or not
            * :type expandSynonyms: bool
            * :param verbose: whether or not output information
            * :type verbose: bool
            * :param test: whether or not to use test data
            * :type test: bool
        """
        # set up and update settings - controls the flow
        self.settings = dict(inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                             inputFilename='WelshGovernmentData21Nov2016.csv',
                             ABpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                             ABfilename='AB.csv',
                             log='ALP',
                             limit=0.1,
                             outname='DataLinking',
                             outpath='/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/',
                             dropColumns=False,
                             expandSynonyms=True,
                             test=False,
                             verbose=False)
        self.settings.update(kwargs)

        # relative path when referring to data files
        self.currentDirectory = os.path.dirname(__file__)  # for relative path definitions

        # define data containers within the object
        self.nExistingUPRN = 0
        self.toLinkAddressData = None
        self.matches = None

        # dictionary container for results - need updating during the processing, mostly in the check_performance
        self.results = dict(date=datetime.datetime.now().strftime("%Y-%m-%d %H%M%S"),
                            name=self.settings['outname'],
                            dataset=self.settings['inputFilename'],
                            addresses=-1,
                            linked=-1,
                            withUPRN=-1,
                            not_linked=-1,
                            correct=-1,
                            false_positive=-1,
                            code_version=__version__)

        # set up a logger, use date and time as filename
        start_date = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.log = logger.set_up_logger(self.settings['log'] + start_date + '.log')
        self.log.info('A new Linking Run Started with the following settings')
        self.log.debug(self.settings)

    def _check_loaded_data(self):
        """
        A simple private method to check what the loaded data contains.

        Computes the number of addresses and those with UPRNs attached. Assumes that
        the old UPRNs are found in UPRN_old column of the dataframe.
        """
        self.log.info('Checking the loaded data...')

        if self.settings['verbose']:
            self.log.info(self.toLinkAddressData.info())

        # count the number of addresses using the index
        naddresses = len(self.toLinkAddressData.index)

        self.log.info('Found {} addresses...'.format(naddresses))
        if 'UPRN_old' in self.toLinkAddressData.columns:
            self.nExistingUPRN = len(self.toLinkAddressData.loc[self.toLinkAddressData['UPRN_old'].notnull()].index)
        else:
            self.log.warning('No existing UPRNs found')
            self.nExistingUPRN = 0

        self.log.info('{} with UPRN already attached...'.format(self.nExistingUPRN))

        self.results['addresses'] = naddresses
        self.results['withUPRN'] = self.nExistingUPRN

        # set index name - needed later for merging / duplicate removal
        self.toLinkAddressData.index.name = 'TestData_Index'

        # update the results dictionary with the number of addresse
        self.results['addresses'] = naddresses
        self.results['withUPRN'] = self.nExistingUPRN

    def load_data(self):
        """
        Read in the data that need to be linked. This method implements only the test file reading and raises
        NotImplementedError otherwise. It is assumed that each input file will require slightly different reading
        method and therefore this method is overwritten after inheritance.

        The implemented load_data method needs to create a Pandas DataFrame self.toLinkAddressData with at least
        one column named ADDRESS. The index of this DataFrame should be called 'TestData_Index' as it is used
        in the class to join the information with AddressBase information.
        """
        if self.settings['test']:
            self.log.warning('Reading in test data...')
            self.settings['inputFilename'] = 'testData.csv'
            self.settings['outname'] = 'DataLinkingTest'

            # update results so that can be filtered out from the database
            self.results['name'] = 'TEST'
            self.results['dataset'] = 'testData.csv'

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
            self.toLinkAddressData.rename(columns={'UPRNs_matched_to_date': 'UPRN_old'}, inplace=True)

            # convert original UPRN to numeric
            self.toLinkAddressData['UPRN_old'] = self.toLinkAddressData['UPRN_old'].convert_objects(
                convert_numeric=True)
        else:
            self.log.info('ERROR - please overwrite the method and make it relevant for the actual data...')
            raise NotImplementedError

    def _load_addressbase(self):
        """
        A private method to load a compressed version of the full AddressBase file. The information being used
        has been processed from a AB Epoch 39 files provided by ONS.

        .. Note: this function modifies the original AB information by e.g. combining different tables. Such
                 activities are undertaken because of the aggressive blocking the prototype linking code uses.
                 The actual production system should take AB as it is and the linking should not perform blocking
                 but rather be flexible and take into account that in NAG the information can be stored in various
                 fields.
        """
        self.log.info('Reading in Address Base Data...')

        if self.settings['test']:
            self.log.warning('Using Test Data...')
            self.settings['ABfilename'] = 'ABtest.csv'

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

        # split postcode to in and outcode - useful for doing blocking in different ways
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

        # if SubBuildingName is empty add dummy - helps as string distance cannot be computed between Nones
        msk = self.addressBase['SUB_BUILDING_NAME'].isnull()
        self.addressBase.loc[msk, 'SUB_BUILDING_NAME'] = 'N/A'

        # set index name - needed later for merging / duplicate removal
        self.addressBase.index.name = 'AddressBase_Index'

    @staticmethod
    def _extract_postcode(string):
        """
        A static private method to extract a postcode from address string.

        Uses a rather loose regular expression, so  may get some strings that are not completely valid postcodes.
        Should not be used to validate whether a postcode conforms to the UK postcode standards.

        The regular expression was taken from:
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
        A private method to normalize address information.

        Removes white spaces, commas, and backslashes. Can also be used to expand common synonyms such
        as RD or BERKS. Finally parses counties as the an early version of the probabilistic parser was
        not trained to parse counties.
        """
        self.log.info('Normalising input addresses')

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
        A private method to address incorrectly parsed London boroughs.

        If the street name contains London borough then move it to locality and remove from the street name.

        :param parsed: a dictionary containing the address tokens that have been parsed
        :type parsed: dict

        :return: a dictionary containing the address tokens with updated information
        :rtype: dict
        """
        london_localities = pd.read_csv(os.path.join(self.currentDirectory, '../../data/') +
                                        'localities.csv')['locality']

        for LondonLocality in london_localities:
            if parsed['StreetName'].strip().endswith(LondonLocality):
                parsed['Locality'] = LondonLocality
                # take the last part out, so that e.g. CHINGFORD AVENUE CHINGFORD is correctly processed
                # need to be careful with e.g.  WESTERN GATEWAY ROYAL VICTORIA DOCK (3 parts to remove)
                parsed['StreetName'] = parsed['StreetName'].strip()[:-len(LondonLocality)].strip()

        return parsed

    def parse_input_addresses_to_tokens(self):
        """
        Parses the address information from the input data.

        Uses a combination of a probabilistic Conditional Random Fields model trained on PAF data and some rules.
        Can perform address string normalisation i.e. remove punctuation and e.g. expand synonyms.
        """
        self.log.info('Start parsing address data...')

        # normalise data so that the parser has the best possible chance of getting things right
        self._normalize_input_data()

        # get addresses and store separately as an vector
        addresses = self.toLinkAddressData['ADDRESS_norm'].values
        self.log.info('{} addresses to parse...'.format(len(addresses)))

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
            # Probabilistic parser should see more cases with london localities, parsed incorrectly at the mo
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
        columns_to_add_empty_strings = ['OrganisationName', 'DepartmentName', 'SubBuildingName', 'BuildingSuffix']
        self.toLinkAddressData[columns_to_add_empty_strings].fillna('', inplace=True)

        # some funky postcodes, remove these
        msk = self.toLinkAddressData['postcode_in'] == 'Z1'
        self.toLinkAddressData.loc[msk, 'postcode_in'] = None
        self.toLinkAddressData.loc[msk, 'Postcode'] = None
        msk = self.toLinkAddressData['postcode_in'] == 'Z11'
        self.toLinkAddressData.loc[msk, 'postcode_in'] = None
        self.toLinkAddressData.loc[msk, 'Postcode'] = None

        # save for inspection
        self.toLinkAddressData.to_csv(self.settings['outpath'] + self.settings['outname'] + '_parsed_addresses.csv',
                                      index=False)

        # drop the temp info
        self.toLinkAddressData.drop(['ADDRESS_norm', ], axis=1, inplace=True)

    def link_all_addresses(self, blocking_modes=(1, 2, 3, 4, None)):
        """
        A method to link addresses against AddressBase.

        :param blocking_modes: a tuple listing all the blocking modes that should be used. These modes correspond to
                               different ways of performing blocking.
        :type blocking_modes: tuple
        """
        self.log.info('Linking addresses against Address Base data...')

        still_missing = self.toLinkAddressData
        for blocking_mode in blocking_modes:
            if len(still_missing.index) > 0:
                new_matches, still_missing = self._find_likeliest_address(still_missing, blocking=blocking_mode)

                if blocking_mode == 1:
                    self.matches = new_matches
                else:
                    self.matches = self.matches.append(new_matches)
            else:
                self.log.info('Found potential matches for all addresses')
                break

    def _find_likeliest_address(self, addresses_to_be_linked, blocking=1):
        """
        A private method to link toMatch data to the AddressBase source information.

        Uses blocking to speed up the matching. This is dangerous if misspelled

        .. note: the aggressive blocking does not work when both BuildingNumber and BuildingName is missing.
                 This is somewhat common for example for care homes. One should really separate these into
                 different category and do blocking only on postcode.

        :param addresses_to_be_linked: dataframe holding the address information that is to be matched against a source
        :type addresses_to_be_linked: pandas.DataFrame
        :param blocking: the mode of blocking, ranging from 1 to 4
        :type blocking: int

        :return: dataframe of matches, dataframe of non-matched addresses
        :rtype: list(pandas.DataFrame, pandas.DataFrame)
        """
        # create pairs
        pcl = rl.Pairs(addresses_to_be_linked, self.addressBase)

        # set blocking - no need to check all pairs, so speeds things up (albeit risks missing if not correctly spelled)
        # block on both postcode and house number, street name can have typos and therefore is not great for blocking
        self.log.info('Start matching with blocking mode {}'.format(blocking))
        if blocking == 1:
            pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'], right_on=['postcode', 'BUILDING_NUMBER'])
        elif blocking == 2:
            pairs = pcl.block(left_on=['Postcode', 'BuildingName'], right_on=['postcode', 'BuildingName'])
        elif blocking == 3:
            pairs = pcl.block(left_on=['BuildingNumber', 'StreetName'], right_on=['BUILDING_NUMBER', 'StreetName'])
        elif blocking == 4:
            pairs = pcl.block(left_on=['BuildingName', 'StreetName'], right_on=['BuildingName', 'StreetName'])
        else:
            pairs = pcl.block(left_on=['BuildingNumber', 'TownName'], right_on=['BUILDING_NUMBER', 'townName'])

        self.log.info('Need to test {0} pairs for {1} addresses...'.format(len(pairs), len(addresses_to_be_linked.index)))

        # compare the two data sets
        # the idea is to build evidence to support linking, hence some fields are compared multiple times
        compare = rl.Compare(pairs, self.addressBase, addresses_to_be_linked, batch=True)

        # set rules for standard residential addresses
        compare.string('SAO_TEXT', 'SubBuildingName', method='damerau_levenshtein', name='flat_dl',
                       missing_value=0.6)
        compare.string('pao_text', 'BuildingName', method='damerau_levenshtein', name='pao_dl',
                       missing_value=0.6)
        compare.string('BuildingName', 'BuildingName', method='damerau_levenshtein', name='building_name_dl',
                       missing_value=0.5)
        compare.string('PAO_START_NUMBER', 'BuildingNumber', method='damerau_levenshtein', name='pao_number_dl',
                       missing_value=0.5)
        compare.string('StreetName', 'StreetName', method='damerau_levenshtein', name='street_dl',
                       missing_value=0.1)
        compare.string('townName', 'TownName', method='damerau_levenshtein', name='town_dl',
                       missing_value=0.2)
        compare.string('locality', 'Locality', method='damerau_levenshtein', name='locality_dl',
                       missing_value=0.5)

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
        compare.string('ORGANISATION_NAME', 'OrganisationName', method='damerau_levenshtein', name='org2_dl',
                       missing_value=0.6)
        compare.string('DEPARTMENT_NAME', 'DepartmentName', method='damerau_levenshtein', name='department_dl',
                       missing_value=0.6)

        # Extras
        compare.string('STREET_DESCRIPTOR', 'StreetName', method='damerau_levenshtein', name='street_desc_dl',
                       missing_value=0.6)

        # execute the comparison model
        compare.run()

        # arbitrarily scale up some of the comparisons - todo: the weights should be solved rather than arbitrary
        compare.vectors['pao_dl'] *= 5.
        compare.vectors['town_dl'] *= 5.
        compare.vectors['sao_number_dl'] *= 4.
        compare.vectors['organisation_dl'] *= 4.
        compare.vectors['flatw_dl'] *= 3.
        compare.vectors['pao_suffix_dl'] *= 2.
        compare.vectors['building_name_dl'] *= 3.
        compare.vectors['locality_dl'] *= 2.

        # add sum of the components to the comparison vectors dataframe
        compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)

        # find all matches where the metrics is above the chosen limit - small impact if choosing the best match
        matches = compare.vectors.loc[compare.vectors['similarity_sum'] > self.settings['limit']]

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
        missingIndex = addresses_to_be_linked.index.difference(matchedIndex)
        missing = addresses_to_be_linked.loc[missingIndex]

        self.log.info('Found {} matches...'.format(len(matches.index)))
        self.log.info('Failed to found {} matches...'.format(len(missing.index)))

        return matches, missing

    def merge_linked_data_and_address_base_information(self):
        """
        Merge address base information to the identified matches.
        """
        self.log.info('Merging back the original information...')

        self.toLinkAddressData = self.toLinkAddressData.reset_index()
        self.matched = pd.merge(self.matches, self.toLinkAddressData, how='left', on='TestData_Index')
        self.matched = pd.merge(self.matched, self.addressBase, how='left', on='AddressBase_Index')

        # drop unnecessary columns
        if self.settings['dropColumns']:
            self.matched.drop(['TestData_Index', 'AddressBase_Index'], axis=1, inplace=True)

    def _run_test(self):
        """
        Run a simple test with a few address that are matched against a mini version of AddressBase.

        Exercises the complete chain from reading in, normalising, parsing, and finally linking.
        Asserts that the linked addresses were correctly linked to counterparts in the mini version of AB.
        """
        self.log.info('Running test...')

        # pandas test whether the UPRNs are the same, ignore type and names, but require exact match
        pdt.assert_series_equal(self.matched['UPRN_old'], self.matched['UPRN'],
                                check_dtype=False, check_exact=True, check_names=False)

    def check_performance(self):
        """
        Check performance.

        Computes the number of linked addresses. If UPRN exists, then calculates the number of
        false positives and those that were not found by the prototype. Splits the numbers based
        on category if present in the data. Finally visualises the results using a simple bar
        chart.
        """
        self.log.info('Checking Performance...')

        # count the number of matches and number of edge cases
        nmatched = len(self.matched.index)
        total = len(self.toLinkAddressData.index)

        # how many were matched
        self.log.info('Matched {} entries'.format(nmatched))
        self.log.info('Total Match Fraction {} per cent'.format(round(nmatched / total * 100., 1)))

        # save matched
        self.matched.to_csv(self.settings['outpath'] + self.settings['outname'] + '_matched.csv', index=False)

        # find those without match
        IDs = self.matched['ID'].values
        missing_msk = ~self.toLinkAddressData['ID'].isin(IDs)
        missing = self.toLinkAddressData.loc[missing_msk]
        not_found = len(missing.index)
        missing.to_csv(self.settings['outpath'] + self.settings['outname'] + '_matched_missing.csv', index=False)
        self.log.info('{} addresses were not linked...'.format(not_found))

        nOldUPRNs = len(self.matched.loc[self.matched['UPRN_old'].notnull()].index)
        self.log.info('{} previous UPRNs in the matched data...'.format(nOldUPRNs))

        # find those with UPRN attached earlier and check which are the same
        msk = self.matched['UPRN_old'] == self.matched['UPRN']
        matches = self.matched.loc[msk]
        sameUPRNs = len(matches.index)
        fp = len(self.matched.loc[~msk].index)
        matches.to_csv(self.settings['outpath'] + self.settings['outname'] + '_sameUPRN.csv', index=False)
        self.log.info('{} addresses have the same UPRN as earlier...'.format(sameUPRNs))

        self.log.info('Correctly Matched {}'.format(sameUPRNs))
        self.log.info('Correctly Matched Fraction {}'.format(round(sameUPRNs / total * 100., 1)))

        self.log.info('False Positives {}'.format(fp))
        self.log.info('False Positive Rate {}'.format(round(fp / total * 100., 1)))

        # find those that has a previous UPRN but does not mach a new one (filter out nulls)
        msk = self.matched['UPRN_old'].notnull()
        notnulls = self.matched.loc[msk]
        nonmatches = notnulls.loc[notnulls['UPRN_old'] != notnulls['UPRN']]
        nonmatches.to_csv(self.settings['outpath'] + self.settings['outname'] + '_differentUPRN.csv', index=False)
        self.log.info('{} addresses have a different UPRN as earlier...'.format(len(nonmatches.index)))

        # find all newly linked
        newUPRNs = self.matched.loc[~msk]
        newUPRNs.to_csv(self.settings['outpath'] + self.settings['outname'] + '_newUPRN.csv', index=False)
        self.log.info('{} more addresses with UPRN...'.format(len(newUPRNs.index)))

        self.results['linked'] = nmatched
        self.results['not_linked'] = not_found
        self.results['correct'] = sameUPRNs
        self.results['false_positive'] = fp

        mne = []
        matchf = []
        fpf = []
        # check results for each class separately if possible
        if 'Category' in self.matched.columns:
            for category in sorted(set(self.matched['Category'].values)):
                msk = (self.matched['UPRN'] == self.matched['UPRN_old']) & (self.matched['Category'] == category)
                correct = self.matched.loc[msk]
                nmatched = len(correct.index)
                outof = len(self.toLinkAddressData.loc[self.toLinkAddressData['Category'] == category].index)
                fp = len(self.matched.loc[(self.matched['UPRN'] != self.matched['UPRN_old']) &
                                          (self.matched['Category'] == category)].index)

                self.log.info('Results for category {}'.format(category))
                self.log.info('Correctly Matched: {}'.format(nmatched))
                self.log.info('Match Fraction: {}'.format(nmatched / outof * 100.))
                self.log.info('False Positives: {}'.format(fp))
                self.log.info('False Positive Rate: {}'.format(fp / outof * 100., 1))

                mne.append(category)
                matchf.append((nmatched / outof * 100.))
                fpf.append(fp / outof * 100.)
        else:
            mne = [self.settings['outname'], ]
            matchf = [sameUPRNs / total * 100., ]
            fpf = [fp / total * 100., ]

        # make a simple visualisation
        x = np.arange(len(mne))
        plt.figure(figsize=(12, 10))

        width = 0.35
        p1 = plt.bar(x, matchf, width, color='g')
        p2 = plt.bar(x + width, fpf, width, color='r')
        plt.ylabel('Fraction of the Sample')
        plt.title('Prototype Linking')
        plt.xticks(x + width, mne, rotation=45)
        plt.ylim(0, 101)
        plt.tight_layout()
        plt.savefig(self.settings['outpath'] + self.settings['outname'] + '.png')
        plt.close()

    def store_results(self, table='results'):
        """
        Stores the results to a SQLite3 database. Appends to the file if it exists.

        :param table: name of the table
        :type table: str

        :return: None
        """
        self.log.info('Storing the results...')

        results = pd.DataFrame.from_records([self.results])

        connection = self.settings['outpath'] + 'AddressLinkingResults.sqlite'

        with sqlite3.connect(connection) as cnx:
            results.to_sql(table, cnx, index=False, if_exists='append')

    def run_all(self):
        """
        Run all required steps to perform parsing and linking using the prototype.

        :return: None
        """
        start = time.clock()
        self.load_data()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        self._check_loaded_data()

        start = time.clock()
        self._load_addressbase()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        start = time.clock()
        self.parse_input_addresses_to_tokens()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        start = time.clock()
        self.link_all_addresses()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        start = time.clock()
        self.addressBase = self.addressBase.reset_index()
        self.merge_linked_data_and_address_base_information()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        self.check_performance()

        self.store_results()

        if self.settings['test']:
            self._run_test()

        self.log.info('Finished running')
        print('Finished!')


if __name__ == "__main__":
    linker = AddressLinker(**dict(test=True))
    linker.run_all()
