"""
ONS Address Index - Probabilistic Parser Common Functions
=========================================================

This file defines the Conditional Random Field parser settings including output file,
structure of the XML expected to hold training data, tokens and features.


Requirements
------------

:requires: pandas
:requires: lxml


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 17-Nov-2016
"""
import pandas as pd
from lxml import etree
import os
import sys
import re
import string


# hardcoded filename and path
MODEL_FILE = 'addressCRF.crfsuite'
directory = os.path.dirname(__file__)  # for relative path definitions
MODEL_PATH = os.path.join(directory, '../training/')
LUT_PATH = '/home/james/Clients/ONS/address-index-api/parsers/src/main/resources/input_pre_post_processing'

# set labels - token names expected in the training file
LABELS = ['OrganisationName',
          'DepartmentName',
          'SubBuildingName',
          'BuildingName',
          'BuildingNumber',
          'StreetName',
          'Locality',
          'TownName',
          'Postcode']

# set some features that are being used to identify tokens
DIRECTIONS = {'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW', 'NORTH', 'SOUTH', 'EAST', 'WEST',
              'NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST'}
FLAT = {'FLAT', 'FLT', 'APARTMENT', 'APPTS', 'APPT', 'APTS', 'APT',
        'ROOM', 'ANNEX', 'ANNEXE', 'UNIT', 'BLOCK', 'BLK'}
COMPANY = {'CIC', 'CIO', 'LLP', 'LP', 'LTD', 'LIMITED', 'CYF', 'PLC', 'CCC', 'UNLTD', 'ULTD'}
ROAD = {'ROAD', 'RAOD', 'RD', 'DRIVE', 'DR', 'STREET', 'STRT', 'AVENUE', 'AVENEU', 'SQUARE',
        'LANE', 'LNE', 'LN', 'COURT', 'CRT', 'CT', 'PARK', 'PK', 'GRDN', 'GARDEN', 'CRESCENT',
        'CLOSE', 'CL', 'WALK', 'WAY', 'TERRACE', 'BVLD', 'HEOL', 'FFORDD', 'PLACE', 'GARDENS',
        'GROVE', 'VIEW', 'HILL', 'GREEN'}
Residential = {'HOUSE', 'HSE', 'FARM', 'LODGE', 'COTTAGE', 'COTTAGES', 'VILLA', 'VILLAS', 'MAISONETTE', 'MEWS'}
Business = {'OFFICE', 'HOSPITAL', 'CARE', 'CLUB', 'BANK', 'BAR', 'UK', 'SOCIETY', 'PRISON', 'HMP', 'RC',
            'UWE', 'UEA', 'LSE', 'KCL', 'UCL', 'UNI', 'UNIV', 'UNIVERSITY', 'UNIVERISTY'}
Locational = {'BASEMENT', 'GROUND', 'UPPER', 'ABOVE', 'TOP', 'LOWER', 'FLOOR', 'HIGHER',
              'ATTIC', 'LEFT', 'RIGHT', 'FRONT', 'BACK', 'REAR', 'WHOLE', 'PART', 'SIDE'}
Ordinal = {'0TH', 'ZEROTH', '0ED', 'SERO', 'SEROFED', 'DIM', 'DIMFED',
          '1ST', 'FIRST', '1AF', 'CYNTA', 'CYNTAF', 'GYNTAF',
          '2ND', 'SECOND', '2AIL', 'AIL', 'AILFED',
          '3RD', 'THIRD', '3YDD', 'TRYDYDD', 'TRYDEDD',
          '4TH', 'FOURTH', '4YDD', 'PEDWERYDD', 'PEDWAREDD',
          '5TH', 'FIFTH', '5ED', 'PUMED',
          '6TH', 'SIXTH', '6ED', 'CHWECHED',
          '7TH', 'SEVENTH', '7FED', 'SEITHFED',
          '8TH', 'EIGHTH', '8FED', 'WYTHFED',
          '9TH', 'NINTH', '9FED', 'NAWFED',
          '10TH', 'TENTH', '10FED', 'DEGFED',
          '11TH', 'ELEVENTH', '11FED', 'UNFED', 'DDEG',
          '12TH', 'TWELFTH', '12FED', 'DEUDDEGFED'}
non_county = {'OFFICE', 'HOSPITAL', 'CARE', 'CLUB', 'BANK', 'BAR', 'SOCIETY', 'PRISON', 
              'HMP', 'UNI', 'UNIV', 'UNIVERSITY', 'UNIVERISTY'}

# Combine these sets for removeCounties function - do not remove counties if they are followed by one of these
noncounty = non_county|COMPANY|FLAT|Residential|ROAD
nonCountyIdentification = list(noncounty)

# Read in the files required for tokenization pre-processing.
with open(LUT_PATH + '/county') as f:
    county = f.read().splitlines()
with open(LUT_PATH + '/synonym') as f:
    synonyms = f.read().splitlines()
# Create a dictionary for the synonyms.
synonym_LUT = dict(map(lambda x: x.split(','), synonyms))

# get some extra info - possible incodes and the linked post towns, used to identify tokens
df = pd.read_csv(os.path.join(directory, '../../data/') + 'postcode_district_to_town.csv')
OUTCODES = set(df['postcode'].values)
POSTTOWNS = set(df['town'].values)

def synonym(token):
    """
    Create a function 'synonym' which will map each of the elements in the synoyms file to the respective synonym.

    :param token: The token to synonymize.
    :type token: string

    :return token_out: The synonym of the token passed in.
    :type: string
    """

    try:
        token_out = synonym_LUT[token]
    except:
        token_out = token

    return token_out

def _stripFormatting(collection):
    """
    Clears formatting for an xml collection.

    :param collection:

    :return:
    """
    collection.text = None
    for element in collection:
        element.text = None
        element.tail = None

    return collection


def readXML(xmlFile):
    """
    Parser XML file and yield the raw text and parsed sequence components.

    :param xmlFile: name of the XML file to parse
    :type xmlFile: str

    :return: raw text, sequence components
    :rtype: list
    """
    component_string_list = []

    # loop through xml file
    if os.path.isfile(xmlFile):
        with open(xmlFile, 'r+') as f:
            tree = etree.parse(f)
            file_xml = tree.getroot()
            file_xml = _stripFormatting(file_xml)
            for component_etree in file_xml:
                # etree components to string representations
                component_string_list.append(etree.tostring(component_etree))
    else:
        print('WARNING: %s does not exist' % xmlFile)
        sys.exit(-9)

    # loop through unique string representations
    for component_string in component_string_list:
        # convert string representation back to xml
        sequence_xml = etree.fromstring(component_string)
        raw_text = etree.tostring(sequence_xml, method='text', encoding='utf-8')
        raw_text = str(raw_text, encoding='utf-8')
        sequence_components = []

        for component in list(sequence_xml):
            sequence_components.append([component.text, component.tag])

        yield raw_text, sequence_components


def digits(token):
    """
    Whether a given string token contains digits or not.

    :param token: input token
    :type token: str

    :return: description of the content
    :rtype: str
    """
    if token.isdigit():
        return 'all_digits'
    elif set(token) & set(string.digits):
        return 'some_digits'
    else:
        return 'no_digits'


def tokenFeatures(token):
    """
    Compute the features for a given string token. Features include the length of the token,
    whether there digits and if the token is directional, residential, business, locational,
    company, road, or posttown or postcode. In addition, check whether the token contains
    vowels and ends in punctuation.

    :param token: input token
    :type token: str

    :return: dictionary of the computed features
    :rtype: dict
    """
    token_clean = token.upper()

    features = {'digits': digits(token_clean),
                'word': (token_clean if not token_clean.isdigit() else False),
                'length': (u'd:' + str(len(token_clean)) if token_clean.isdigit() else u'w:' + str(len(token_clean))),
                'endsinpunc': (token[-1] if bool(re.match('.+\.$', token, flags=re.UNICODE)) else False),
                'directional': token_clean in DIRECTIONS,
                'outcode': token_clean in OUTCODES,
                'posttown': token_clean in POSTTOWNS,
                'has.vowels': bool(set(token_clean) & set('AEIOU')),
                'flat': token_clean in FLAT,
                'company': token_clean in COMPANY,
                'road': token_clean in ROAD,
                'residential': token_clean in Residential,
                'business': token_clean in Business,
                'locational': token_clean in Locational,
                'ordinal': token_clean in Ordinal,
                'hyphenations': token_clean.count('-') # how many dashes, like 127-129 or bradford-on-avon
                }

    return features


def tokens2features(tokens):
    """
    This should call tokenFeatures to get features for individual tokens,
    as well as define any features that are dependent upon tokens before/after.

    :param tokens:
    :return:
    """
    feature_sequence = [tokenFeatures(tokens[0])]
    previous_features = feature_sequence[-1].copy()

    for token in tokens[1:]:
        # set features for individual tokens (calling tokenFeatures)
        token_features = tokenFeatures(token)
        current_features = token_features.copy()

        # features for the features of adjacent tokens
        feature_sequence[-1]['next'] = current_features
        token_features['previous'] = previous_features

        # DEFINE ANY OTHER FEATURES THAT ARE DEPENDENT UPON TOKENS BEFORE/AFTER
        # for example, a feature for whether a certain character has appeared previously in the token sequence

        feature_sequence.append(token_features)
        previous_features = current_features

    if len(feature_sequence) > 1:
        # these are features for the tokens at the beginning and end of a string
        feature_sequence[0]['rawstring.start'] = True
        feature_sequence[-1]['rawstring.end'] = True
        feature_sequence[1]['previous']['rawstring.start'] = True
        feature_sequence[-2]['next']['rawstring.end'] = True

    else:
        # a singleton feature, for if there is only one token in a string
        feature_sequence[0]['singleton'] = True

    return feature_sequence

def replaceSynonyms(tokens):
    """
    This function replaces all of the words in the synonym list with their synonyms.

    :param tokens: the list of tokens to replace with synonyms.
    :type tokens: list of strings.

    :return tokens: the synonymized list.
    :type iterator: iterateas throught the list of strings.
    """

    tokens = map(lambda x: synonym(x), tokens)

    return tokens

def removeCounties(in_string):
    """
    This function will remove any counties which appear in the county list.

    :param in_string: the string from which to remove the counties.
    :type in_string: str

    :return out_string: the input string with the counties removed.
    :type out_string: str
    """

    # Step 1 - ON, DINAS and UPON or a number - if a county follows one of these
    # words (eg. Bradford on Avon, Dinas Powys or Stratford upon Avon, or a 5 Somerset),
    # do not remove it
    c_except = [r"ON\s",r"DINAS\s",r"UPON\s",r"[0-9]\s"]

    # Step 2 - Do look behind for ON, DINAS and POWYS
    look_behind = r"(?<!\b{0})({1})".format(r")(?<!\b".join(c_except), "|".join(county))

    # Step 3 - Do look ahead. nonCountyIdentification is a list of words (eg.
    # ROAD, STREET etc.). We don't want to remove counties if they're followed by
    # road or street
    a = r"\b|\s".join(nonCountyIdentification)
    look_ahead = r'(?!(\s{sepSuffixes}\b))'.format(sepSuffixes = a)

    # Step 4 - Combine and run
    final_regex = look_behind + look_ahead
    out_string = re.compile(final_regex).sub('', in_string)

    return out_string

def tokenize(raw_string):
    """
    The function determines how any given string is split into its tokens.
    Uses regular expression to split a given string to tokens.

    :param raw_string: an unprocessed string
    :type raw_string: str or bytes

    :return: a list of tokens
    :rtype: list
    """

    if isinstance(raw_string, bytes):
        try:
            raw_string = str(raw_string, encoding='utf-8')
        except:
            raw_string = str(raw_string)

    # Normalize the input string according to the pre-processing in the beta.

    # Convert to uppercase.
    upperInput = raw_string.upper()
    inputWithoutCounties = removeCounties(upperInput)

    # Do the regular expression replacements as per the scala parsing.
    regex1 = re.compile("(\\d+[A-Z]?) *- *(\\d+[A-Z]?)")
    tokens = regex1.sub("\g<1>-\g<2>", inputWithoutCounties)

    regex2 = re.compile("(\\d+)/(\\d+)")
    tokens = regex2.sub("\g<1>-\g<2>", tokens)

    regex3 = re.compile("(\\d+) *TO *(\\d+)")
    tokens = regex3.sub("\g<1>-\g<2>", tokens)

    # Do the non-regular expression replacements.
    tokens = tokens.replace(" IN ", " ").replace(" CO ", " ").replace(" - ", " ").replace(",", " ").replace("\\", " ")

    # Now split the string by whitespace.
    tokens = tokens.split()

    # Replace any synonyms from the synonyms list and remove any counties that are now discoverable.
    preprocessed_string = removeCounties(' '.join(replaceSynonyms(tokens)))

    re_tokens = re.compile(r"\(*\b[^\s,;#&()]+[.,;)\n]* | [#&]", re.VERBOSE | re.UNICODE)
    tokens = re_tokens.findall(preprocessed_string)

    if not tokens:
        return []

    return tokens


def readData(xmlFile):
    """
    Parses the specified data files and returns it in sklearn format.

    :param xmlFile: name of the XML file to parse
    :type xmlFile: str

    :return: features (X) and labels (y)
    """
    data = readXML(xmlFile)

    X = []
    y = []
    for raw_string, components in data:
        tokens, labels = list(zip(*components))
        X.append(tokens2features(tokens))
        y.append(labels)

    return X, y
