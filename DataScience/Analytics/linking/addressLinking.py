"""
ONS Address Index - Linking Prototype
=====================================

Contains a class, which implements an Address Linking Prototype (ALP).

This is a prototype code aimed for experimentation and testing. There are not unit tests, but
a simple functional test that performs a simple validation on the complete parsing and linking
chain.

The code has been written for speed rather than accuracy, it therefore uses fairly aggressive
blocking. As the final solution will likely use ElasticSearch, the aim of this prototype is
not the highest accuracy but to quickly test different ideas, which can inform the final
ElasticSearch solution.

.. Warning:: The current version relies strongly on postcode. If the address has an incorrect
             postcode, it is fairly likely that an incorrect address will be linked. There are
             two options to address this in the prototype: 1) one should add e.g. Town or street
             name to the blocking to use the postcode only if it matches the Town name, or 2)
             one should try to validate during the parsing if the postcode is likely to be
             correct or not.


Requirements
------------

:requires: ProbabilisticParser (a CRF model specifically build for ONS)
:requires: pandas (0.19.1)
:requires: numpy (1.11.2)
:requires: tqdm (4.10.0: https://github.com/tqdm/tqdm)
:requires: recordlinkage (0.6.0: https://pypi.python.org/pypi/recordlinkage/)
:requires: matplotlib (1.5.3)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.9
:date: 1-Feb-2017
"""
import datetime
import os
import re
import sqlite3
import time
import warnings
import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import recordlinkage as rl
from Analytics.linking import logger
from ProbabilisticParser import parser
from tqdm import tqdm
import matplotlib

matplotlib.use('Agg')  # to prevent Tkinter crashing on cdhut-d03
import matplotlib.pyplot as plt

warnings.simplefilter(action="ignore", category=FutureWarning)
pd.options.mode.chained_assignment = None

__version__ = '0.9'


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
            * :param limit: Minimum probability for a potential match to be included in the list of potential matches.
                            Affects for example the false positive rate.
            * :type limit: float
            * :param outname: a string that is prepended to the output data
            * :type outname: str
            * :param outpath: location to which to store the output data
            * :type outpath: str
            * :param multipleMatches: whether or not to store multiple matches or just the likeliest
            * :type multipleMatches: bool
            * :param dropColumns: whether or not to drop extra columns that are created during the linking
            * :type dropColumns: bool
            * :param expandSynonyms: whether to expand common synonyms or not
            * :type expandSynonyms: bool
            * :param expandPostcode: whether to expand a postcode to in and out codes or not
            * :type expandPostcode: bool
            * :param test: whether or not to use test data
            * :type test: bool
            * :param store: whether or not to store the results to a database table
            * :type store: bool
            * :param verbose: whether or not output information
            * :type verbose: bool
        """
        # set up and update settings - controls the flow
        self.settings = dict(inputPath='/Users/saminiemi/Projects/ONS/AddressIndex/data/',
                             inputFilename='WelshGovernmentData21Nov2016.csv',
                             ABpath='/Users/saminiemi/Projects/ONS/AddressIndex/data/ADDRESSBASE/',
                             ABfilename='AB_processed.csv',
                             limit=0.0,
                             outname='DataLinking',
                             outpath='/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/',
                             multipleMatches=False,
                             dropColumns=True,
                             expandSynonyms=True,
                             expandPostcode=True,
                             test=False,
                             store=True,
                             verbose=False)
        self.settings.update(kwargs)

        # relative path when referring to data files
        self.currentDirectory = os.path.dirname(__file__)  # for relative path definitions

        # define data containers within the object, should be instantiated in __init__
        self.nExistingUPRN = 0
        self.toLinkAddressData = pd.DataFrame()
        self.matches = pd.DataFrame()
        self.addressBase = pd.DataFrame()
        self.matching_results = pd.DataFrame()
        self.matched_results = pd.DataFrame()

        if self.settings['test']:
            self.settings['outname'] = 'DataLinkingTest'

        # dictionary container for results - need updating during the processing, mostly in the check_performance
        self.results = dict(date=datetime.datetime.now(),
                            name=self.settings['outname'],
                            dataset=self.settings['inputFilename'],
                            addresses=-1,
                            linked=-1,
                            withUPRN=-1,
                            not_linked=-1,
                            correct=-1,
                            false_positive=-1,
                            new_UPRNs=-1,
                            code_version=__version__)

        # set up a logger, use date and time as filename
        start_date = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.log = logger.set_up_logger(self.settings['outpath'] + self.settings['outname'] + '_' + start_date + '.log')
        self.log.info('A new Linking Run Started with the following settings')
        self.log.debug(self.settings)

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

            # update results so that can be filtered out from the database
            self.results['name'] = 'TEST'
            self.results['dataset'] = 'testData.csv'

            self.toLinkAddressData = pd.read_csv(self.settings['inputPath'] + self.settings['inputFilename'],
                                                 low_memory=False, comment='#')

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
            self.log.info('ERROR - please overwrite the method and make it relevant for the actual test data...')
            raise NotImplementedError

    def check_loaded_data(self):
        """
        A simple method to check what the loaded data contains.

        Computes the number of addresses and those with UPRNs attached. Assumes that
        the old UPRNs are found in UPRN_old column of the dataframe.
        """
        self.log.info('Checking the loaded data...')

        # count the number of addresses using the index
        n_addresses = len(self.toLinkAddressData.index)

        self.log.info('Found {} addresses...'.format(n_addresses))

        if 'UPRN_old' in self.toLinkAddressData.columns:
            self.nExistingUPRN = len(self.toLinkAddressData.loc[self.toLinkAddressData['UPRN_old'].notnull()].index)
        else:
            self.log.warning('No existing UPRNs found')
            self.nExistingUPRN = 0

        self.log.info('{} with UPRN already attached...'.format(self.nExistingUPRN))

        self.results['addresses'] = n_addresses
        self.results['withUPRN'] = self.nExistingUPRN

        # set index name - needed later for merging / duplicate removal
        self.toLinkAddressData.index.name = 'TestData_Index'

        # update the results dictionary with the number of addresses
        self.results['addresses'] = n_addresses
        self.results['withUPRN'] = self.nExistingUPRN

        if self.settings['verbose']:
            print('Input File:')
            print(self.toLinkAddressData.info(verbose=True, memory_usage=True, null_counts=True))

    def load_addressbase(self):
        """
        A method to load a compressed version of the full AddressBase file.

        The information being used has been processed from a AB Epoch 39 files provided by ONS.

        .. Note: this method assumes that all modifications have already been carried out. This method
                 allows the prototype to be run on the ONS utility node as the memory requirements are
                 reduced compared to when using the load_and_process_addressbase method.
        """
        self.log.info('Reading in Modified Address Base Data...')

        self.addressBase = pd.read_csv(self.settings['ABpath'] + self.settings['ABfilename'],
                                       dtype={'UPRN': np.int64, 'ORGANISATION_NAME': str,
                                              'DEPARTMENT_NAME': str, 'SUB_BUILDING_NAME': str, 'BUILDING_NAME': str,
                                              'BUILDING_NUMBER': str, 'THROUGHFARE': str,
                                              'POST_TOWN': str, 'POSTCODE': str, 'PAO_TEXT': str,
                                              'PAO_START_NUMBER': str, 'PAO_START_SUFFIX': str,
                                              'PAO_END_SUFFIX': str, 'PAO_END_NUMBER': str, 'SAO_START_SUFFIX': str,
                                              'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                                              'STREET_DESCRIPTOR': str, 'TOWN_NAME': str, 'LOCALITY': str,
                                              'postcode_in': str, 'postcode_out': str})

        self.addressBase['PAO_START_NUMBER'] = self.addressBase['PAO_START_NUMBER'].fillna('-12345')
        self.addressBase['PAO_START_NUMBER'] = self.addressBase['PAO_START_NUMBER'].astype(np.int32)

        self.addressBase['PAO_END_NUMBER'] = self.addressBase['PAO_END_NUMBER'].fillna('-12345')
        self.addressBase['PAO_END_NUMBER'] = self.addressBase['PAO_END_NUMBER'].astype(np.int32)

        # normalise street names so that st. is always st and 's is always s - PAF and NLP has differences
        msk = self.addressBase['THROUGHFARE'].str.contains('ST\.\s', na=False, case=False)
        self.addressBase.loc[msk, 'THROUGHFARE'] = self.addressBase.loc[msk, 'THROUGHFARE'].str.replace('ST\. ', 'ST ')
        msk = self.addressBase['THROUGHFARE'].str.contains("'S\s", na=False, case=False)
        self.addressBase.loc[msk, 'THROUGHFARE'] = self.addressBase.loc[msk, 'THROUGHFARE'].str.replace("'S\s", 'S ')

        self.log.info('Using {} addresses from AddressBase for matching...'.format(len(self.addressBase.index)))

        # set index name - needed later for merging / duplicate removal
        self.addressBase.index.name = 'AddressBase_Index'

    def load_and_process_addressbase(self):
        """
        A method to load a compressed version of the full AddressBase file and to process it.

        .. Note: this method modifies the original AB information by e.g. combining different tables. Such
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
                                              'PAO_END_SUFFIX': str, 'SAO_TEXT': str, 'SAO_START_NUMBER': np.float64,
                                              'SAO_START_SUFFIX': str, 'ORGANISATION': str, 'STREET_DESCRIPTOR': str,
                                              'TOWN_NAME': str, 'LOCALITY': str})
        self.log.info('Found {} addresses from AddressBase...'.format(len(self.addressBase.index)))

        # remove street records from the list of potential matches - this makes the search space slightly smaller
        exclude = 'STREET RECORD|ELECTRICITY SUB STATION|PUMPING STATION|POND \d+M FROM|PUBLIC TELEPHONE|'
        exclude += 'PART OF OS PARCEL|DEMOLISHED BUILDING|CCTV CAMERA|TANK \d+M FROM|SHELTER \d+M FROM|TENNIS COURTS|'
        exclude += 'PONDS \d+M FROM|SUB STATION'
        msk = self.addressBase['PAO_TEXT'].str.contains(exclude, na=False, case=False)
        self.addressBase = self.addressBase.loc[~msk]

        # combine information - could be done differently, but for now using some of these for blocking
        msk = self.addressBase['THROUGHFARE'].isnull()
        self.addressBase.loc[msk, 'THROUGHFARE'] = self.addressBase.loc[msk, 'STREET_DESCRIPTOR']

        msk = self.addressBase['BUILDING_NUMBER'].isnull()
        self.addressBase.loc[msk, 'BUILDING_NUMBER'] = self.addressBase.loc[msk, 'PAO_START_NUMBER']

        msk = self.addressBase['BUILDING_NAME'].isnull()
        self.addressBase.loc[msk, 'BUILDING_NAME'] = self.addressBase.loc[msk, 'PAO_TEXT']

        msk = self.addressBase['ORGANISATION_NAME'].isnull()
        self.addressBase.loc[msk, 'ORGANISATION_NAME'] = self.addressBase.loc[msk, 'ORGANISATION']

        msk = self.addressBase['POSTCODE'].isnull()
        self.addressBase.loc[msk, 'POSTCODE'] = self.addressBase.loc[msk, 'POSTCODE_LOCATOR']

        msk = self.addressBase['SUB_BUILDING_NAME'].isnull()
        self.addressBase.loc[msk, 'SUB_BUILDING_NAME'] = self.addressBase.loc[msk, 'SAO_TEXT']

        msk = self.addressBase['POST_TOWN'].isnull()
        self.addressBase.loc[msk, 'POST_TOWN'] = self.addressBase.loc[msk, 'TOWN_NAME']

        msk = self.addressBase['POSTCODE'].isnull()
        self.addressBase.loc[msk, 'POSTCODE'] = self.addressBase.loc[msk, 'POSTCODE_LOCATOR']

        msk = self.addressBase['LOCALITY'].isnull()
        self.addressBase.loc[msk, 'LOCALITY'] = self.addressBase.loc[msk, 'DEPENDENT_LOCALITY']

        # sometimes addressbase does not have SAO_START_NUMBER even if SAO_TEXT clearly has a number
        # take the digits from SAO_TEXT and place them to SAO_START_NUMBER if this is empty
        msk = self.addressBase['SAO_START_NUMBER'].isnull() & (self.addressBase['SAO_TEXT'].notnull())
        self.addressBase.loc[msk, 'SAO_START_NUMBER'] = pd.to_numeric(
            self.addressBase.loc[msk, 'SAO_TEXT'].str.extract('(\d+)'))
        self.addressBase['SAO_START_NUMBER'].fillna(value=-12345, inplace=True)
        self.addressBase['SAO_START_NUMBER'] = self.addressBase['SAO_START_NUMBER'].astype(np.int32)

        self.addressBase['PAO_START_NUMBER'] = self.addressBase['PAO_START_NUMBER'].fillna('-12345')
        self.addressBase['PAO_START_NUMBER'] = self.addressBase['PAO_START_NUMBER'].astype(np.int32)

        self.addressBase['PAO_END_NUMBER'] = self.addressBase['PAO_END_NUMBER'].fillna('-12345')
        self.addressBase['PAO_END_NUMBER'] = self.addressBase['PAO_END_NUMBER'].astype(np.int32)

        # normalise street names so that st. is always st and 's is always s
        msk = self.addressBase['THROUGHFARE'].str.contains('ST\.\s', na=False, case=False)
        self.addressBase.loc[msk, 'THROUGHFARE'] = self.addressBase.loc[msk, 'THROUGHFARE'].str.replace('ST\. ', 'ST ')
        msk = self.addressBase['THROUGHFARE'].str.contains("'S\s", na=False, case=False)
        self.addressBase.loc[msk, 'THROUGHFARE'] = self.addressBase.loc[msk, 'THROUGHFARE'].str.replace("'S\s", 'S ')

        # drop some that are not needed - in the future versions these might be useful
        self.addressBase.drop(['DEPENDENT_LOCALITY', 'POSTCODE_LOCATOR', 'ORGANISATION'],
                              axis=1, inplace=True)

        # split postcode to in and outcode - useful for doing blocking in different ways
        if self.settings['expandPostcode']:
            postcodes = self.addressBase['POSTCODE'].str.split(' ', expand=True)
            postcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
            self.addressBase = pd.concat([self.addressBase, postcodes], axis=1)

        self.log.info('Using {} addresses from AddressBase for matching...'.format(len(self.addressBase.index)))

        # set index name - needed later for merging / duplicate removal
        self.addressBase.index.name = 'AddressBase_Index'

        if self.settings['verbose']:
            print('AddressBase:')
            print(self.addressBase.info(verbose=True, memory_usage=True, null_counts=True))
            if not self.settings['test']:
                self.addressBase.to_csv(self.settings['ABpath'] + 'AB_processed.csv', index=False)

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
            potential_postcode = re.findall(regx, string)[0][0]
            potential_postcode = potential_postcode.lower().strip()
        except IndexError:
            potential_postcode = None

        # above regex gives also those without space between, add if needed
        if potential_postcode is not None:
            if ' ' not in potential_postcode:
                inc = potential_postcode[-3:]
                out = potential_postcode.replace(inc, '')
                potential_postcode = out + ' ' + inc

        return potential_postcode

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
                                                                              x['ADDRESS_norm'].replace(', ', ' '),
                                                                              axis=1)
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData.apply(lambda x:
                                                                              x['ADDRESS_norm'].replace(',', ' '),
                                                                              axis=1)

        # remove backslash if present and replace with space
        self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData.apply(lambda x:
                                                                              x['ADDRESS_norm'].replace('\\', ' '),
                                                                              axis=1)

        # remove spaces around hyphens as this causes ranges to be interpreted incorrectly
        # e.g. FLAT 15 191 - 193 NEWPORT ROAD CARDIFF CF24 1AJ is parsed incorrectly if there
        # is space around the hyphen
        self.toLinkAddressData['ADDRESS_norm'] = \
            self.toLinkAddressData.apply(lambda x: x['ADDRESS_norm'].replace('\d+ - \d+', '\d+-\d+'), axis=1)

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
        addRegex = '(?:\s)(?!ROAD|LANE|STREET|CLOSE|DRIVE|AVENUE|SQUARE|COURT|PARK|CRESCENT|WAY|WALK|HEOL|FFORDD|HILL|GARDENS|GATE|GROVE|HOUSE|VIEW|BUILDING|VILLAS|LODGE|PLACE|ROW|WHARF|RISE|TERRACE|CROSS|ENTERPRISE|HATCH|&)'

        # remove county from address but add a column for it
        self.toLinkAddressData['County'] = None
        for county in counties:
            msk = self.toLinkAddressData['ADDRESS_norm'].str.contains(county + addRegex, regex=True, na=False)
            self.toLinkAddressData.loc[msk, 'County'] = county
            self.toLinkAddressData['ADDRESS_norm'] = self.toLinkAddressData['ADDRESS_norm'].str.replace(county +
                                                                                                        addRegex, '',
                                                                                                        case=False)

    @staticmethod
    def _fix_london_boroughs(parsed, directory, datafile='localities.csv'):
        """
        A static private method to address incorrectly parsed London boroughs.

        If the street name contains London borough then move it to locality and remove from the street name.

        :param parsed: a dictionary containing the address tokens that have been parsed
        :type parsed: dict
        :param directory: location of the data file
        :type directory: str
        :param datafile: name of the data file containing a column locality
        :type datafile: str

        :return: a dictionary containing the address tokens with updated information
        :rtype: dict
        """
        london_localities = pd.read_csv(directory + datafile)['locality']

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
        sub_building = []
        flat_number = []
        building_name = []
        building_number = []
        pao_start_number = []
        pao_end_number = []
        building_suffix = []
        street = []
        locality = []
        town = []
        postcode = []

        # loop over addresses - quite inefficient, should avoid a loop
        for address in tqdm(addresses):
            parsed = parser.tag(address.upper())  # probabilistic parser
            possible_postcode = self._extract_postcode(address)  # regular expression extraction

            # if both parsers found postcode then check that they are the same
            if parsed.get('Postcode', None) is not None and possible_postcode is not None:
                if parsed['Postcode'] != possible_postcode:
                    # not the same, use possible_postcode
                    parsed['Postcode'] = possible_postcode

            # if the probabilistic parser did not find postcode but regular expression did, then use that
            if parsed.get('Postcode', None) is None and possible_postcode is not None:
                parsed['Postcode'] = possible_postcode

            if parsed.get('Postcode', None) is not None:
                # check that there is space, if not then add if the parsed postcode is long enough to contain a complete
                # postcode. Some users have partial postcodes to which one should not add a space.
                if ' ' not in parsed['Postcode'] and len(parsed['Postcode']) > 4:
                    in_code = parsed['Postcode'][-3:]
                    out_code = parsed['Postcode'].replace(in_code, '')
                    parsed['Postcode'] = out_code + ' ' + in_code

                # change to all capitals
                parsed['Postcode'] = parsed['Postcode'].upper()

            # if Hackney etc. in StreetName then remove and move to locality if town name contains London
            # Probabilistic parser should see more cases with london localities, parsed incorrectly at the mo
            if parsed.get('StreetName', None) is not None and parsed.get('TownName', None) is not None:
                if 'LONDON' in parsed['TownName']:
                    parsed = self._fix_london_boroughs(parsed, os.path.join(self.currentDirectory, '../../data/'))

            # if delivery point address is e.g. "5 BEST HOUSE", then the "5" refers likely to FLAT 5
            if parsed.get('BuildingNumber', None) is None and parsed.get('BuildingName', None) is not None:
                tmp = parsed['BuildingName'].split(' ')
                if len(tmp) > 1:
                    try:
                        _ = int(tmp[0])
                        parsed['FlatNumber'] = tmp[0]
                    except ValueError:
                        pass

            # if BuildingName is e.g. 55A then should get the number and suffix separately
            if parsed.get('BuildingName', None) is not None:

                parsed['pao_end_number'] = None

                if '-' in parsed['BuildingName']:
                    tmp = parsed['BuildingName'].split('-')
                    parsed['pao_start_number'] = ''.join([x for x in tmp[0] if x.isdigit()])
                    parsed['pao_end_number'] = ''.join([x for x in tmp[-1] if x.isdigit()])
                else:
                    parsed['pao_start_number'] = ''.join([x for x in parsed['BuildingName'] if x.isdigit()])

                if len(parsed['pao_start_number']) < 1:
                    parsed['pao_start_number'] = None

                parsed['BuildingSuffix'] = ''.join([x for x in parsed['BuildingName'] if not x.isdigit()])

                # accept suffixes that are only maximum two chars and if not hyphen
                if len(parsed['BuildingSuffix']) > 2 or parsed['BuildingSuffix'] == '-' or \
                                parsed['BuildingSuffix'] == '/':
                    parsed['BuildingSuffix'] = None

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

            # if pao_start_number is Null then add BuildingNumber to it
            if parsed.get('pao_start_number', None) is None and parsed.get('BuildingNumber', None) is not None:
                parsed['pao_start_number'] = parsed['BuildingNumber']

            # parser sometimes places house to organisation name, while it is likelier that it should be subBuilding
            if parsed.get('OrganisationName') == 'HOUSE' and parsed.get('SubBuildingName', None) is None:
                parsed['SubBuildingName'] = parsed.get('OrganisationName')

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
            building_suffix.append(parsed.get('BuildingSuffix', None))
            pao_start_number.append(parsed.get('pao_start_number', None))
            pao_end_number.append(parsed.get('pao_end_number', None))
            flat_number.append(parsed.get('FlatNumber', None))

        # add the parsed information to the dataframe
        self.toLinkAddressData['OrganisationName'] = organisation
        self.toLinkAddressData['DepartmentName'] = department
        self.toLinkAddressData['SubBuildingName'] = sub_building
        self.toLinkAddressData['BuildingName'] = building_name
        self.toLinkAddressData['BuildingNumber'] = building_number
        self.toLinkAddressData['StreetName'] = street
        self.toLinkAddressData['Locality'] = locality
        self.toLinkAddressData['TownName'] = town
        self.toLinkAddressData['Postcode'] = postcode
        self.toLinkAddressData['BuildingSuffix'] = building_suffix
        self.toLinkAddressData['BuildingStartNumber'] = pao_start_number
        self.toLinkAddressData['BuildingEndNumber'] = pao_end_number
        self.toLinkAddressData['FlatNumber'] = flat_number
        self.toLinkAddressData['PAOText'] = self.toLinkAddressData['BuildingName'].copy()
        self.toLinkAddressData['SAOText'] = self.toLinkAddressData['SubBuildingName'].copy()

        if self.settings['expandPostcode']:
            # if valid postcode information found then split between in and outcode
            if self.toLinkAddressData['Postcode'].count() > 0:
                postcodes = self.toLinkAddressData['Postcode'].str.split(' ', expand=True)
                postcodes.rename(columns={0: 'postcode_in', 1: 'postcode_out'}, inplace=True)
                self.toLinkAddressData = pd.concat([self.toLinkAddressData, postcodes], axis=1)
            else:
                self.toLinkAddressData['postcode_in'] = None
                self.toLinkAddressData['postcode_out'] = None

        # if building number is empty and subBuildingName is a only number, add to BuildingStartNumber
        msk = self.toLinkAddressData['SubBuildingName'].str.contains('\d+', na=False, case=False) & \
              self.toLinkAddressData['BuildingStartNumber'].isnull()
        self.toLinkAddressData.loc[msk, 'BuildingStartNumber'] = self.toLinkAddressData.loc[msk, 'SubBuildingName']

        # split flat or apartment number as separate for numerical comparison - compare e.g. SAO number
        msk = self.toLinkAddressData['SubBuildingName'].str.contains('flat|apartment|unit', na=False, case=False)
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = self.toLinkAddressData.loc[msk, 'SubBuildingName']
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = \
            self.toLinkAddressData.loc[msk].apply(lambda x: x['FlatNumber'].strip().
                                                  replace('FLAT', '').replace('APARTMENT', '').replace('UNIT', ''),
                                                  axis=1)

        # sometimes subBuildingName is e.g. C2 where to number refers to the flat number
        msk = self.toLinkAddressData['FlatNumber'].str.contains('[A-Z]\d+', na=False, case=False)
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = \
            self.toLinkAddressData.loc[msk, 'FlatNumber'].str.replace('[A-Z]', '')

        # deal with addresses that are of type 5/7 4 whatever road...
        msk = self.toLinkAddressData['SubBuildingName'].str.contains('\d+\/\d+', na=False, case=False) & \
              self.toLinkAddressData['FlatNumber'].isnull() & self.toLinkAddressData['BuildingNumber'].notnull()
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = \
            self.toLinkAddressData.loc[msk, 'SubBuildingName'].str.replace('\/\d+', '')

        # some addresses have / as the separator for buildings and flats, when matching against NLP, needs "FLAT"
        msk = self.toLinkAddressData['SubBuildingName'].str.contains('\d+\/\d+', na=False, case=False)
        self.toLinkAddressData.loc[msk, 'SubBuildingName'] = 'FLAT ' + \
                                                             self.toLinkAddressData.loc[msk, 'SubBuildingName']

        # if SubBuildingName contains only numbers, then place also to the flat number field as likely to be flat
        msk = self.toLinkAddressData['SubBuildingName'].str.isnumeric() & self.toLinkAddressData['FlatNumber'].isnull()
        msk[msk.isnull()] = False
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = self.toLinkAddressData.loc[msk, 'SubBuildingName']

        # some addresses, e.g. "5B ELIZABETH AVENUE", have FLAT implicitly even if not spelled -> add "FLAT X"
        msk = (self.toLinkAddressData['BuildingSuffix'].notnull()) & \
              (self.toLinkAddressData['SubBuildingName'].isnull())
        self.toLinkAddressData.loc[msk, 'SubBuildingName'] = 'FLAT ' + self.toLinkAddressData.loc[msk, 'BuildingSuffix']

        # in some other cases / is in the BuildingName field - now this separates the building and flat
        # the first part refers to the building number and the second to the flat
        msk = self.toLinkAddressData['BuildingName'].str.contains('\d+\/\d+', na=False, case=False) & \
              self.toLinkAddressData['FlatNumber'].isnull()
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = self.toLinkAddressData.loc[msk, 'BuildingName']
        self.toLinkAddressData.loc[msk, 'FlatNumber'] = \
            self.toLinkAddressData.loc[msk, 'FlatNumber'].str.replace('\d+\/', '')
        self.toLinkAddressData['FlatNumber'] = pd.to_numeric(self.toLinkAddressData['FlatNumber'], errors='coerce')
        self.toLinkAddressData['FlatNumber'].fillna(-12345, inplace=True)
        self.toLinkAddressData['FlatNumber'] = self.toLinkAddressData['FlatNumber'].astype(np.int32)

        self.toLinkAddressData.loc[msk, 'BuildingStartNumber'] = self.toLinkAddressData.loc[msk, 'BuildingName']
        self.toLinkAddressData.loc[msk, 'BuildingStartNumber'] = \
            self.toLinkAddressData.loc[msk, 'BuildingStartNumber'].str.replace('\/\d+', '')
        self.toLinkAddressData['BuildingStartNumber'] = pd.to_numeric(self.toLinkAddressData['BuildingStartNumber'],
                                                                      errors='coerce')
        self.toLinkAddressData['BuildingStartNumber'].fillna(-12345, inplace=True)
        self.toLinkAddressData['BuildingStartNumber'] = self.toLinkAddressData['BuildingStartNumber'].astype(np.int32)

        self.toLinkAddressData['BuildingEndNumber'] = pd.to_numeric(self.toLinkAddressData['BuildingEndNumber'],
                                                                    errors='coerce')
        self.toLinkAddressData['BuildingEndNumber'].fillna(-12345, inplace=True)
        self.toLinkAddressData['BuildingEndNumber'] = self.toLinkAddressData['BuildingEndNumber'].astype(np.int32)

        # if SubBuilding name or BuildingSuffix is empty add dummy - helps when comparing against None
        msk = self.toLinkAddressData['SubBuildingName'].isnull()
        self.toLinkAddressData.loc[msk, 'SubBuildingName'] = 'N/A'
        msk = self.toLinkAddressData['BuildingSuffix'].isnull()
        self.toLinkAddressData.loc[msk, 'BuildingSuffix'] = 'N/A'
        msk = self.toLinkAddressData['PAOText'].isnull()
        # for some welsh addresses the building name is parsed as organisation name, so place to PAOtext if empty
        self.toLinkAddressData.loc[msk, 'PAOText'] = self.toLinkAddressData['OrganisationName']
        msk = self.toLinkAddressData['PAOText'].isnull()
        self.toLinkAddressData.loc[msk, 'PAOText'] = ''
        msk = self.toLinkAddressData['SAOText'].isnull()
        self.toLinkAddressData.loc[msk, 'SAOText'] = 'N/A'

        # fill columns that are often NA with empty strings - helps when doing string comparisons against Nones
        columns_to_add_empty_strings = ['OrganisationName', 'DepartmentName', 'SubBuildingName']
        self.toLinkAddressData[columns_to_add_empty_strings].fillna('', inplace=True)

        # save for inspection
        self.toLinkAddressData.to_csv(self.settings['outpath'] + self.settings['outname'] + '_parsed_addresses.csv',
                                      index=False)

        # drop the temp info
        self.toLinkAddressData.drop(['ADDRESS_norm', ], axis=1, inplace=True)

        if self.settings['verbose']:
            print('Parsed:')
            print(self.toLinkAddressData.info(verbose=True, memory_usage=True, null_counts=True))

    def link_all_addresses(self, blocking_modes=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)):
        """
        A method to link addresses against AddressBase.

        :param blocking_modes: a tuple listing all the blocking modes that should be used. These modes correspond to
                               different ways of performing blocking.
        :type blocking_modes: tuple
        """
        self.log.info('Linking addresses against Address Base data...')

        still_missing = self.toLinkAddressData
        all_new_matches = []

        # loop over the different blocking modes to find all matches
        for blocking_mode in tqdm(blocking_modes):
            if len(still_missing.index) > 0:
                new_matches, still_missing = self._find_likeliest_address(still_missing, blocking=blocking_mode)
                all_new_matches.append(new_matches)
            else:
                continue  # using continue here because break does not allow tqdm to finish

        # concatenate all the new matches to a single dataframe
        self.matches = pd.concat(all_new_matches)

    def _find_likeliest_address(self, addresses_to_be_linked, blocking=1):
        """
        A private method to link addresses_to_be_linked data to the AddressBase source information.

        Uses different blocking methods to speed up the matching. This is dangerous if any information is misspelled.
        Note also that this method simply returns the likeliest matching address if multiple matches are found.
        This is to facilitate automated scoring. In practice one should set a limit above which all potential
        matches are returned and either perform postprocessing clustering or return all potential matches to the
        user for manual inspection.

        :param addresses_to_be_linked: dataframe holding the address information that is to be matched against a source
        :type addresses_to_be_linked: pandas.DataFrame
        :param blocking: the mode of blocking, ranging from 1 to 8
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
            pairs = pcl.block(left_on=['OrganisationName', 'TownName', 'BuildingNumber'],
                              right_on=['ORGANISATION_NAME', 'POST_TOWN', 'BUILDING_NUMBER'])
        elif blocking == 2:
            pairs = pcl.block(left_on=['OrganisationName', 'TownName'],
                              right_on=['ORGANISATION_NAME', 'POST_TOWN'])
        elif blocking == 3:
            pairs = pcl.block(left_on=['Postcode', 'BuildingName'],
                              right_on=['POSTCODE', 'BUILDING_NAME'])
        elif blocking == 4:
            pairs = pcl.block(left_on=['Postcode', 'BuildingNumber'],
                              right_on=['POSTCODE', 'BUILDING_NUMBER'])
        elif blocking == 5:
            pairs = pcl.block(left_on=['Postcode', 'StreetName'],
                              right_on=['POSTCODE', 'THROUGHFARE'])
        elif blocking == 6:
            pairs = pcl.block(left_on=['Postcode', 'TownName'],
                              right_on=['POSTCODE', 'POST_TOWN'])
        elif blocking == 7:
            pairs = pcl.block(left_on=['Postcode'],
                              right_on=['POSTCODE'])
        elif blocking == 8:
            pairs = pcl.block(left_on=['BuildingName', 'StreetName'],
                              right_on=['BUILDING_NAME', 'THROUGHFARE'])
        elif blocking == 9:
            pairs = pcl.block(left_on=['BuildingNumber', 'StreetName'],
                              right_on=['BUILDING_NUMBER', 'THROUGHFARE'])
        elif blocking == 10:
            pairs = pcl.block(left_on=['StreetName', 'TownName'],
                              right_on=['THROUGHFARE', 'POST_TOWN'])
        else:
            pairs = pcl.block(left_on=['BuildingNumber', 'TownName'],
                              right_on=['BUILDING_NUMBER', 'POST_TOWN'])

        self.log.info(
            'Need to test {0} pairs for {1} addresses...'.format(len(pairs), len(addresses_to_be_linked.index)))

        # compare the two data sets
        # the idea is to build evidence to support linking, hence some fields are compared multiple times
        compare = rl.Compare(pairs, self.addressBase, addresses_to_be_linked, batch=True)

        # set rules for standard residential addresses
        compare.string('SAO_TEXT', 'SAOText', method='jarowinkler', name='flat_dl',
                       missing_value=0.6)
        compare.string('PAO_TEXT', 'PAOText', method='jarowinkler', name='pao_dl',
                       missing_value=0.6)
        compare.string('BUILDING_NAME', 'BuildingName', method='jarowinkler', name='building_name_dl',
                       missing_value=0.8)
        compare.string('BUILDING_NUMBER', 'BuildingNumber', method='jarowinkler', name='building_number_dl',
                       missing_value=0.5)
        compare.numeric('PAO_START_NUMBER', 'BuildingStartNumber', threshold=0.1, method='linear',
                        name='pao_number_dl')
        compare.numeric('PAO_END_NUMBER', 'BuildingEndNumber', threshold=0.1, method='linear',
                        name='building_end_number_dl')
        compare.string('THROUGHFARE', 'StreetName', method='jarowinkler', name='street_dl',
                       missing_value=0.7)
        compare.string('POST_TOWN', 'TownName', method='jarowinkler', name='town_dl',
                       missing_value=0.2)
        compare.string('LOCALITY', 'Locality', method='jarowinkler', name='locality_dl',
                       missing_value=0.5)

        # add a comparison of the incode - this helps with e.g. life events addresses
        if self.settings['expandPostcode']:
            compare.string('postcode_in', 'postcode_in', method='jarowinkler', name='incode_dl',
                           missing_value=0.0)
            compare.string('postcode_out', 'postcode_out', method='jarowinkler', name='outcode_dl',
                           missing_value=0.0)

        if blocking in (1, 2, 8, 9, 10):
            compare.string('POSTCODE', 'Postcode', method='jarowinkler', name='postcode_dl',
                           missing_value=0.0)

        # use to separate e.g. 55A from 55
        compare.string('PAO_START_SUFFIX', 'BuildingSuffix', method='jarowinkler', name='pao_suffix_dl',
                       missing_value=0.5)

        # the following is good for flats and apartments, which have been numbered
        compare.string('SUB_BUILDING_NAME', 'SubBuildingName', method='jarowinkler', name='flatw_dl',
                       missing_value=0.6)
        compare.numeric('SAO_START_NUMBER', 'FlatNumber', threshold=0.1, method='linear', name='sao_number_dl')

        # set rules for organisations such as care homes and similar type addresses
        compare.string('ORGANISATION_NAME', 'OrganisationName', method='jarowinkler', name='organisation_dl',
                       missing_value=0.1)
        compare.string('DEPARTMENT_NAME', 'DepartmentName', method='jarowinkler', name='department_dl',
                       missing_value=0.6)

        # Extras
        compare.string('STREET_DESCRIPTOR', 'StreetName', method='jarowinkler', name='street_desc_dl',
                       missing_value=0.6)

        # execute the comparison model
        compare.run()

        # remove those matches that are not close enough - requires e.g. street name to be close enough
        if blocking in (1, 2):
            compare.vectors = compare.vectors.loc[compare.vectors['incode_dl'] >= 0.8]
            compare.vectors = compare.vectors.loc[compare.vectors['outcode_dl'] >= 0.5]
        elif blocking in (3, 4):
            compare.vectors = compare.vectors.loc[compare.vectors['street_dl'] >= 0.5]
        elif blocking in (5, 6, 7):
            compare.vectors = compare.vectors.loc[compare.vectors['pao_number_dl'] > 0.9]
        elif blocking in (6, 7):
            msk = (compare.vectors['street_dl'] >= 0.7) | (compare.vectors['organisation_dl'] > 0.3)
            compare.vectors = compare.vectors.loc[msk]

        # upweight organisation name and building numbers
        compare.vectors['organisation_dl'] *= 3.
        compare.vectors['pao_dl'] *= 2.
        compare.vectors['building_number_dl'] *= 2.
        compare.vectors['pao_number_dl'] *= 2.
        compare.vectors['building_end_number_dl'] *= 2.

        # compute the sum of similarities
        compare.vectors['similarity_sum'] = compare.vectors.sum(axis=1)

        # find all matches where the probability is above the limit - filters out low prob links
        matches = compare.vectors.loc[compare.vectors['similarity_sum'] > self.settings['limit']]

        # reset index
        matches.reset_index(inplace=True)

        # to pick the most likely match we sort by the sum of the similarity and pick the top
        # sort matches by the sum of the vectors and then keep the first
        matches.sort_values(by=['similarity_sum', 'AddressBase_Index'], ascending=[False, True], inplace=True)

        # add blocking mode
        matches['block_mode'] = blocking

        if not self.settings['multipleMatches']:
            matches.drop_duplicates('TestData_Index', keep='first', inplace=True)

        # matched IDs
        matched_index = matches['TestData_Index'].values

        # missing ones
        missing_index = addresses_to_be_linked.index.difference(matched_index)
        missing = addresses_to_be_linked.loc[missing_index]

        self.log.info('Found {} potential matches...'.format(len(matches.index)))
        self.log.info('Failed to found matches for {} addresses...'.format(len(missing.index)))

        return matches, missing

    def merge_linked_data_and_address_base_information(self):
        """
        Merge address base information to the identified matches, sort by the likeliest match, and
        drop other potential matches.
        """
        self.log.info('Merging back the original information...')

        self.toLinkAddressData.reset_index(inplace=True)

        self.matches.sort_values(by='similarity_sum', ascending=False, inplace=True)

        # remove those not needed from address base before merging
        address_base_index = self.matches['AddressBase_Index'].values
        self.addressBase = self.addressBase.loc[self.addressBase['AddressBase_Index'].isin(address_base_index)]

        # perform actual matching of matches and address base
        self.matching_results = pd.merge(self.toLinkAddressData, self.matches, how='left', on='TestData_Index',
                                         copy=False)
        self.matching_results = pd.merge(self.matching_results, self.addressBase, how='left', on='AddressBase_Index',
                                         copy=False)

        # sort by similarity, save for inspection and keep only the likeliest
        if self.settings['multipleMatches']:
            self.matching_results.to_csv(self.settings['outpath'] + self.settings['outname'] + '_all_matches.csv',
                                         index=False)
            self.matching_results.drop_duplicates('TestData_Index', keep='first', inplace=True)

        # drop unnecessary columns
        if self.settings['dropColumns']:
            self.matching_results.drop(['TestData_Index', 'AddressBase_Index'], axis=1, inplace=True)

    def _run_test(self):
        """
        A private method to run a simple test with a few address that are matched against a mini version of AddressBase.

        Exercises the complete chain from reading in, normalising, parsing, and finally linking.
        Asserts that the linked addresses were correctly linked to counterparts in the mini version of AB.
        """
        self.log.info('Running test...')

        # pandas test whether the UPRNs are the same, ignore type and names, but require exact match
        pdt.assert_series_equal(self.matching_results['UPRN_old'], self.matching_results['UPRN'],
                                check_dtype=False, check_exact=True, check_names=False)

    def check_performance(self):
        """
        A method to compute the linking performance.

        Computes the number of linked addresses. If UPRNs exist, then calculates the number of
        false positives and those that were not found by the prototype. Splits the numbers based
        on category if present in the data. Finally visualises the results using a simple bar
        chart.
        """
        self.log.info('Checking Performance...')

        # count the number of matches and the total number of addresses and write to the lod
        total = len(self.matching_results.index)

        # save matched to a file for inspection
        self.matching_results.to_csv(self.settings['outpath'] + self.settings['outname'] + '.csv', index=False)
        if 'UPRN_old' in self.matching_results.columns:
            columns = ['ID', 'UPRN_old', 'ADDRESS', 'UPRN']
        else:
            columns = ['ID', 'ADDRESS', 'UPRN']
        tmp = self.matching_results[columns]
        tmp.rename(columns={'UPRN_old': 'UPRN_prev', 'UPRN': 'UPRN_new'}, inplace=True)
        tmp.to_csv(self.settings['outpath'] + self.settings['outname'] + '_minimal.csv', index=False)

        msk = self.matching_results['UPRN'].isnull()
        self.matched_results = self.matching_results.loc[~msk]
        self.matched_results.to_csv(self.settings['outpath'] + self.settings['outname'] + '_matched.csv', index=False)
        n_matched = len(self.matched_results.index)

        # find those without match and write to the log and file
        missing = self.matching_results.loc[msk]
        not_found = len(missing.index)
        missing.to_csv(self.settings['outpath'] + self.settings['outname'] + '_matched_missing.csv', index=False)

        self.log.info('Matched {} entries'.format(n_matched))
        self.log.info('Total Match Fraction {} per cent'.format(round(n_matched / total * 100., 1)))
        self.log.info('{} addresses were not linked...'.format(not_found))

        # if UPRN_old is present then check the overlap and the number of false positives
        if 'UPRN_old' not in self.matching_results.columns:
            true_positives = -1
            false_positives = -1
            n_new_UPRNs = -1
        else:
            # find those with UPRN attached earlier and check which are the same
            msk = self.matched_results['UPRN_old'] == self.matched_results['UPRN']
            matches = self.matched_results.loc[msk]
            true_positives = len(matches.index)
            matches.to_csv(self.settings['outpath'] + self.settings['outname'] + '_sameUPRN.csv', index=False)

            self.log.info('{} previous UPRNs in the matched data...'.format(self.nExistingUPRN))
            self.log.info('{} addresses have the same UPRN as earlier...'.format(true_positives))
            self.log.info('Correctly Matched {}'.format(true_positives))
            self.log.info('Correctly Matched Fraction {}'.format(round(true_positives / total * 100., 1)))

            # find those that have previous UPRNs but do not match the new ones (filter out nulls)
            msk = self.matched_results['UPRN_old'].notnull()
            not_nulls = self.matched_results.loc[msk]
            non_matches = not_nulls.loc[not_nulls['UPRN_old'] != not_nulls['UPRN']]
            false_positives = len(non_matches.index)
            non_matches.to_csv(self.settings['outpath'] + self.settings['outname'] + '_differentUPRN.csv', index=False)

            self.log.info('{} addresses have a different UPRN as earlier...'.format(false_positives))
            self.log.info('False Positives {}'.format(false_positives))
            self.log.info('False Positive Rate {}'.format(round(false_positives / total * 100., 1)))

            # get precision, recall and f1-score
            try:
                precision = true_positives / (true_positives + false_positives)
            except ZeroDivisionError:
                # in some rare cases with a few existing UPRNs it can happen that the union of new and old is empty
                precision = 0.
            recall = true_positives / total  # note that this is not truly recall as some addresses may have no match
            try:
                f1score = 2. * (precision * recall) / (precision + recall)
            except ZeroDivisionError:
                f1score = 0.

            self.log.info('Precision = {}'.format(precision))
            self.log.info('Minimum Recall = {}'.format(recall))
            self.log.info('Minimum F1-score = {}'.format(f1score))

            # find all newly linked - those that did not have UPRNs already attached
            new_UPRNs = self.matched_results.loc[~msk]
            n_new_UPRNs = len(new_UPRNs.index)
            new_UPRNs.to_csv(self.settings['outpath'] + self.settings['outname'] + '_newUPRN.csv', index=False)
            self.log.info('{} more addresses with UPRN...'.format(n_new_UPRNs))

        self.results['linked'] = n_matched
        self.results['not_linked'] = not_found
        self.results['correct'] = true_positives
        self.results['false_positive'] = false_positives
        self.results['new_UPRNs'] = n_new_UPRNs

        # make a simple visualisation
        all_results = [total, n_matched, true_positives, n_new_UPRNs, false_positives, not_found]
        all_results_names = ['Input', 'Linked', 'Same UPRNs', 'New UPRNs', 'Different UPRNs', 'Not Linked']
        self._generate_performance_figure(all_results, all_results_names)

        # check results for each class separately if possible
        if 'Category' in self.matching_results.columns:
            for category in sorted(set(self.matched_results['Category'].values)):
                msk = (self.matched_results['UPRN'] == self.matched_results['UPRN_old']) & \
                      (self.matched_results['Category'] == category)

                true_positives = self.matched_results.loc[msk]
                n_true_positives = len(true_positives.index)
                outof = len(self.toLinkAddressData.loc[self.toLinkAddressData['Category'] == category].index)
                false_positives = len(
                    self.matched_results.loc[(self.matched_results['UPRN'] != self.matched_results['UPRN_old']) &
                                             (self.matched_results['Category'] == category)].index)

                self.log.info('Results for category {}'.format(category))
                self.log.info('Correctly Matched: {}'.format(n_true_positives))
                self.log.info('Match Fraction: {}'.format(n_true_positives / outof * 100.))
                self.log.info('False Positives: {}'.format(false_positives))
                self.log.info('False Positive Rate: {}'.format(round(false_positives / outof * 100., 1)))

                try:
                    precision = n_true_positives / (n_true_positives + false_positives)
                except ZeroDivisionError:
                    # in some rare cases with a few existing UPRNs it can happen that the union of new and old is Null
                    precision = 0.
                recall = n_true_positives / outof
                try:
                    f1score = 2. * (precision * recall) / (precision + recall)
                except ZeroDivisionError:
                    f1score = 0.

                self.log.info('Precision = {}'.format(precision))
                self.log.info('Minimum Recall = {}'.format(recall))
                self.log.info('Minimum F1-score = {}'.format(f1score))

    def _generate_performance_figure(self, all_results, all_results_names, width=0.5):
        """
        A simple bar chart showing the performance.

        If existing UPRNs are available then simple metrics like the number of false positives is also shown.

        :param all_results: a list of number of addresses in a given category as specified by all_results_names
        :type all_results: list
        :param all_results_names: a list containing the names that will be used as y-ticks
        :type all_results_names: list
        :param width: relative bar width
        :type width: float

        :return: None
        """
        location = np.arange(len(all_results))

        fig = plt.figure(figsize=(12, 10))
        plt.title('Prototype Linking Code (version={})'.format(__version__))
        ax = fig.add_subplot(1, 1, 1)

        max_bar_length = max(all_results)
        plt.barh(location, all_results, width, color='g', alpha=0.6)

        for patch in ax.patches:
            if patch.get_x() < 0:
                continue

            n_addresses = int(patch.get_width())
            ratio = n_addresses / max_bar_length

            if ratio > 0.3:
                ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                            xytext=(-95, 18), textcoords='offset points', color='white', fontsize=24)
            else:
                ax.annotate("%i" % n_addresses, (patch.get_x() + patch.get_width(), patch.get_y()),
                            xytext=(10, 18), textcoords='offset points', color='black', fontsize=24)

        plt.xlabel('Number of Addresses')
        plt.yticks(location, all_results_names)
        plt.xlim(0, ax.get_xlim()[1] * 1.02)
        plt.tight_layout()
        plt.savefig(self.settings['outpath'] + self.settings['outname'] + '.png')
        plt.close()

    def store_results(self, table='results'):
        """
        Stores the results to a SQLite3 database. Appends to the database table if it exists.

        :param table: name of the database table to store the results
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
        self.check_loaded_data()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        start = time.clock()
        if self.settings['test']:
            self.load_and_process_addressbase()
        else:
            self.load_addressbase()

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
        self.addressBase.reset_index(inplace=True)
        self.merge_linked_data_and_address_base_information()
        stop = time.clock()
        self.log.info('finished in {} seconds...'.format(round((stop - start), 1)))

        self.check_performance()

        if self.settings['store']:
            self.store_results()

        if self.settings['test']:
            self._run_test()

        self.log.info('Finished running')
        print('Finished!')


if __name__ == "__main__":
    linker = AddressLinker(**dict(test=True, multipleMatches=True, store=False, verbose=True))
    linker.run_all()
