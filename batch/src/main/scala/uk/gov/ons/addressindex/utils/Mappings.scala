package uk.gov.ons.addressindex.utils


object Mappings {
  val hybrid =
  """
   {
     "settings": {
       "number_of_shards": 4,
       "number_of_replicas": 1,
       "index.queries.cache.enabled": false,
       "index": {
         "similarity": {
           "default": {
             "type": "classic"
           }
         }
       },
       "analysis": {
         "filter": {
           "address_synonym_filter": {
             "type": "synonym",
             "synonyms": [
                  "ADJ, ADJACENT",
                  "ALY => ALLEY, ALY",
                  "ANX, ANNEX, ANNEXE",
                  "APPT, APT, APARTMENT, FLT, FLAT", 
                  "APPTS, APTS, APARTMENTS, FLTS, FLATS",
                  "ARC => ARCADE, ARC",
                  "AV => AVENUE, AV",
                  "AVE => AVENUE, AVE",
                  "AVEN => AVENUE, AVEN",
                  "BCH => BEACH, BCH",
                  "BDWY => BROADWAY, BDWY",
                  "BG => BURG, BG",
                  "BLD => BUILDING, BUILDINGS, BLD",
                  "BLDG => BUILDING, BUILDINGS, BLDG",
                  "BLDS => BUILDINGS, BLDS",
                  "BLDGS => BUILDINGS, BLDGS",
                  "BLF => BLUFF, BLF",
                  "BLK => BLOCK, BLK",
                  "BLVD => BOULEVARD, BLVD", 
                  "BND => BEND, BND",
                  "BR => BRANCH, BR",
                  "BRG => BRIDGE, BRG",
                  "BRK => BROOK, BRK",
                  "BSMT => BASEMENT, BSMT",
                  "BTM => BOTTOM, BTM",
                  "BYP => BYPASS, BYP",
                  "CIR => CIRCLE, CIRCUS, CIR",
                  "CIRC => CIRCLE, CIRCUS, CIRC",
                  "CL => CLOSE, CL",
                  "CLB => CLUB, CLB",
                  "CLF => CLIFF, CLF",
                  "CLFS => CLIFFS, CLFS",
                  "CLS => CLOSE, CLS",
                  "COR => CORNER, COR",
                  "CORS => CORNERS, CORS",
                  "CP => CAMP, CP",
                  "CPE => CAPE, CPE",
                  "CR => CRESCENT, CR",
                  "CRES => CRESCENT, CRES",
                  "CRK => CREEK, CRK",
                  "CRSE => COURSE, CRSE",
                  "CRT => COURT, CRT",
                  "CT => COURT, CT",
                  "CTR => CENTER, CENTRE, CTR",
                  "CTS => COURTS, CTS",
                  "DL => DALE, DL",
                  "DR => DRIVE, DR",
                  "DRV => DRIVE, DRV",
                  "DV => DIVIDE, DV",
                  "EST => ESTATE, EST",
                  "EXT => EXTENSION, EXT",
                  "FL => FLOOR, FL",
                  "FLD => FIELD, FLD",
                  "FLDS => FIELDS, FLDS",
                  "FLR => FLOOR, FLR",
                  "FLS => FALLS, FLS",
                  "FRD => FORD, FRD",
                  "FRNT => FRONT, FRNT",
                  "FRST => FOREST, FRST",
                  "FRY => FERRY, FRY",
                  "FT => FORT, FT",
                  "GDN => GARDEN, GDN",
                  "GDNS => GARDENS, GDNS",
                  "GLN => GLEN, GLN",
                  "GN => GREEN, GN",
                  "GR => GROVE, GR",
                  "GRD, GRND, GROUND",
                  "GRDN => GARDEN, GRDN",
                  "GRDNS => GARDENS, GRDNS",
                  "GRN => GREEN, GRN",
                  "GRV => GROVE, GRV",
                  "GTWY => GATEWAY, GTWY",
                  "HBR => HARBOR, HBR",
                  "HL => HILL, HL",
                  "HLS => HILLS, HLS",
                  "HNGR => HANGER, HNGR",
                  "HOLW => HOLLOW, HOLW",
                  "HSE => HOUSE, HSE",
                  "HTS => HEIGHTS, HTS",
                  "HVN => HAVEN, HVN",
                  "IS => ISLAND, IS",
                  "ISS => ISLANDS, ISS",
                  "JCT => JUNCTION, JCT",
                  "LBBY => LOBBY, LBBY",
                  "LCKS => LOCK, LCKS",
                  "LCKS => LOCKS, LCKS",
                  "LDG => LODGE, LDG",
                  "LK => LAKE, LK",
                  "LKS => LAKES, LKS",
                  "LN => LANE, LN",
                  "LNDG => LANDING, LNDG",
                  "LNE => LANE, LNE, LN",
                  "LOWR => LOWER, LOWR",
                  "MDWS => MEADOWS, MDWS",
                  "ML => MILL, ML",
                  "MLS => MILLS, MLS",
                  "MNR => MANOR, MNR",
                  "MSN => MISSION, MSN",
                  "MT => MOUNT, MT",
                  "MTN => MOUNTAIN, MTN",
                  "NO, NUMBER",
                  "NOS, NUMBERS",
                  "ORCH => ORCHARD, ORCH",
                  "PH => PENTHOUSE, PH",
                  "PK => PARK, PK",
                  "PKWY => PARKWAY, PKWY",
                  "PL => PLACE, PL",
                  "PLN => PLAIN, PLN",
                  "PLNS => PLAINS, PLNS",
                  "PLZ => PLAZA, PLZ",
                  "PNE => PINE, PNE",
                  "PNES => PINES, PNES",
                  "PR => PRAIRIE, PR",
                  "PRT => PORT, PRT",
                  "PT => POINT, PT",
                  "QRY => QUARRY, QRY",
                  "RADL => RADIAL, RADL",
                  "RD => ROAD, RD",
                  "RDG => RIDGE, RDG",
                  "RIV => RIVER, RIV",
                  "RM => ROOM, RM",
                  "RNCH => RANCH, RNCH",
                  "RPD => RAPID, RPD",
                  "RPDS => RAPIDS, RPDS",
                  "RST => REST, RST",
                  "SHR => SHORE, SHR",
                  "SHRS => SHORES, SHRS",
                  "SMT => SUMMIT, SMT",
                  "SPC => SPACE, SPC",
                  "SPG => SPRING, SPG",
                  "SPGS => SPRINGS, SPGS",
                  "SQ => SQUARE, SQ",
                  "ST => STREET, ST",
                  "STA => STATION, STA",
                  "STE => SUITE, STE",
                  "STR => STREET, STR",
                  "STRM => STREAM, STRM",
                  "TER => TERRACE, TER",
                  "TRAK => TRACK, TRAK",
                  "TRCE => TRACE, TRCE",
                  "TRL => TRAIL, TRL",
                  "TRLR => TRAILER, TRLR",
                  "TUNL => TUNNEL, TUNL",
                  "UN => UNION, UN",
                  "UPPR => UPPER, UPPR",
                  "VAL => VALLEY, VAL",
                  "VIA => VIADUCT, VIA",
                  "VIS => VISTA, VIS",
                  "VL => VILLE, VL",
                  "VLG => VILLAGE, VLG",
                  "VLY => VALLEY, VLY",
                  "VW => VIEW, VW",
                  "WAY => WAY, WAY",
                  "WLK => WALK, WLK",
                  "WLS => WELL, WLS",
                  "WLS => WELLS, WLS",
                  "XING => CROSSING, XING",

                  "DEPT => DEPARTMENT, DEPT",
                  "OFC => OFFICE, OFC",
                  "LLC, LLP => LTD, LIMITED, LLC, LLP",
                  "LTD, LIMITED, CYF",
                  "UNLTD, ULTD, UNLIMITED",
                  "CO, COMPANY",
                  "PLC, CCC => CO, COMPANY, PLC, CCC",
                  "CORP, CORPORATION",
                  "INC, INCOMPORATED, CORPORATION",

                  "E => EAST, E",
                  "W => WEST, W",
                  "S => SOUTH, S",
                  "N => NORTH, N" ,

                  "SAINT => ST, SAINT, SANT",
                  "SANT => ST, SAINT, SANT",
                  "0TH, ZEROTH, 0ED, SERO, SEROFED, DIM, DIMFED",
                  "1ST, FIRST, 1AF, CYNTA, CYNTAF, GYNTAF",
                  "2ND, SECOND, 2AIL, AIL, AILFED",
                  "3RD, THIRD, 3YDD, TRYDYDD, TRYDEDD",
                  "4TH, FOURTH, 4YDD, PEDWERYDD, PEDWAREDD",
                  "5TH, FIFTH, 5ED, PUMED",
                  "6TH, SIXTH, 6ED, CHWECHED",
                  "7TH, SEVENTH, 7FED, SEITHFED",
                  "8TH, EIGHTH, 8FED, WYTHFED",
                  "9TH, NINTH, 9FED, NAWFED",
                  "10TH, TENTH, 10FED, DEGFED",
                  "11TH, ELEVENTH, 11FED, UNFED, DDEG",
                  "12TH, TWELFTH, 12FED, DEUDDEGFED",

                  "CAREHOME => CARE HOME, CAREHOME, RESIDENTIAL HOME, NURSING HOME, RETIREMENT HOME",
                  "CARE, RESIDENTIAL, NURSING, RETIREMENT",
                  "HMP, HM PRISON",
                  "UNI, UNIV, UNIVERSITY",
                  "B&B, BED AND BREAKFAST, BED&BREAKFAST",
                  "&, AND",
                  "PUB, PH, PUBLIC HOUSE",
                  "FC, F.C, FOOTBALL CLUB",
                  "YHA, YOUTH HOSTEL ASSOCIATION", 
                  "CE, C OF E, COFE, CHURCH OF ENGLAND",
                  "RC, ROMAN CATHOLIC",
                  "URC, UNITED REFORM CHURCH, UNITED REFORMED CHURCH",
                  "GSM LONDON, GREENWICH SCHOOL OF MANAGEMENT",
                  "JUNIOR,  J",
                  "INFANT, I",
                  "J&I, I&J, PRIMARY",
                  "YSGOL, SCHOOL",
                  "KCL, KINGS COLLEGE LONDON",
                  "LSE, LONDON SCHOOL OF ECONOMICS",
                  "MMU, MANCHESTER METROPOLITAN UNIVERSITY",
                  "UCL, UNIVERSITY COLLEGE LONDON",
                  "QMUL, QUEEN MARY UNIVERSITY OF LONDON",
                  "RADA, ROYAL ACADEMY OF DRAMATIC ARTS",
                  "RHUL, ROYAL HOLLOWAY UNIVERSITY OF LONDON",
                  "SOAS, SCHOOL OF ORIENTAL AND AFRICAN STUDIES",
                  "TVU, THAMES VALLEY UNIVERSITY",
                  "UC, UNIVERSITY COLLEGE",
                  "UCLAN, UNIVERSITY OF CENTRAL LANCASHIRE",
                  "UCS, UNIVERSITY CAMPUS SUFFOLK",
                  "UEA, UNIVERSITY OF EAST ANGLIA",
                  "UEL, UNIVERSITY OF EAST LONDON",
                  "UKC, UNIVERSITY OF KENT AT CANTERBURY",
                  "UWA, UNIVERSITY OF ABERYSTWYTH",
                  "UWE, UNIVERSITY OF THE WEST OF ENGLAND",
                  "UWIC, CARDIFF METROPOLITAN UNIVERSITY",
                  "UWS, UNIVERSITY OF THE WEST OF SCOTLAND",
                  "SPORTS, LEISURE, SPORT",
                  "FARM, FARMHOUSE",
                  "BUSINESS, INDUSTRIAL",
                  "DLR, DOCKLANDS LIGHT RAILWAY",
                  "POST, DELIVERY"
              ]
           },
           "shingle_filter": {
             "type": "shingle",
             "min_shingle_size": 2,
             "max_shingle_size": 2,
             "output_unigrams": false
           },
           "english_poss_stemmer": {
              "type": "stemmer",
              "name": "possessive_english"
           },
           "edge_ngram": {
             "type": "edgeNGram",
             "min_gram": "1",
             "max_gram": "25",
             "token_chars": ["letter", "digit"]
           }
         },
         "analyzer": {
           "welsh_no_split_analyzer": {
             "tokenizer": "custom_keyword",
             "filter": [
               "asciifolding"
             ]
           },
           "welsh_split_analyzer": {
             "tokenizer": "standard",
             "filter": [
               "asciifolding"
             ]
           },
           "welsh_split_synonyms_analyzer": {
             "tokenizer": "standard",
             "filter": [
               "asciifolding",
               "address_synonym_filter"
             ]
           },
           "welsh_bigram_analyzer": {
             "type": "custom",
             "tokenizer": "standard",
             "filter": [
               "asciifolding",
               "shingle_filter"
             ]
           },
           "edge_ngram_analyzer": {
             "filter": ["lowercase", "english_poss_stemmer", "edge_ngram"],
             "tokenizer": "standard"
           },
           "keyword_analyzer": {
             "filter": ["lowercase", "english_poss_stemmer"],
             "tokenizer": "standard"
           }
         },
         "tokenizer": {
           "custom_keyword": {
             "type": "keyword",
             "buffer_size": 256
           }
         }
       }
     },
     "mappings": {
       "address": {
         "properties": {
           "lpi": {
             "properties": {
               "addressBasePostal": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "classificationCode": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "easting": {
                 "type": "float",
                 "index": "false"
               },
               "location": {
                 "type": "geo_point",
                 "index": "true"
               },
               "legalName": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer"
               },
               "level": {
                 "type": "text",
                 "index": "false"
               },
               "locality": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "lpiLogicalStatus": {
                 "type": "byte",
                 "index": "true"
               },
               "blpuLogicalStatus": {
                 "type": "byte",
                 "index": "true"
               },
               "lpiKey": {
                 "type": "text",
                 "index": "false"
               },
               "northing": {
                 "type": "float",
                 "index": "false"
               },
               "officialFlag": {
                 "type": "text",
                 "index": "false"
               },
               "organisation": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer"
               },
               "paoEndNumber": {
                 "type": "short",
                 "index": "true"
               },
               "paoEndSuffix": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "paoStartNumber": {
                 "type": "short",
                 "index": "true"
               },
               "paoStartSuffix": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword",
                 "fields": {
                   "keyword": {
                     "type": "keyword"
                   }
                 }
               },
               "paoText": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer"
               },
               "postcodeLocator": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "saoEndNumber": {
                 "type": "short",
                 "index": "true"
               },
               "saoEndSuffix": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "saoStartNumber": {
                 "type": "short",
                 "index": "true"
               },
               "saoStartSuffix": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "saoText": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer"
               },
               "streetDescriptor": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer",
                 "fields": {
                   "keyword": {
                     "type": "keyword"
                   }
                 }
               },
               "townName": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "uprn": {
                 "type": "long",
                 "index": "false"
               },
               "usrn": {
                 "type": "integer",
                 "index": "false"
               },
               "parentUprn": {
                 "type": "long",
                 "index": "false"
               },
               "multiOccCount": {
                 "type": "short",
                 "index": "false"
               },
               "localCustodianCode": {
                 "type": "short",
                 "index": "false"
               },
               "rpc": {
                 "type": "byte",
                 "index": "false"
               },
               "usrnMatchIndicator": {
                 "type": "byte",
                 "index": "false"
               },
               "language": {
                 "type": "text",
                 "index": "false"
               },
               "streetClassification": {
                 "type": "byte",
                 "index": "false"
               },
               "classScheme": {
                 "type": "text",
                 "index": "false"
               },
               "nagAll": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer",
                 "fields": {
                   "bigram": {
                     "type": "text",
                     "analyzer": "welsh_bigram_analyzer"
                   },
                    "partial": {
                      "search_analyzer": "keyword_analyzer",
                      "type": "text",
                      "analyzer": "edge_ngram_analyzer"
                    }
                 }
               },
               "lpiStartDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "true"
               },
               "lpiLastUpdateDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "false"
               },
               "lpiEndDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "true",
                 "null_value": "2021-03-31T00:00:00Z"
               },
               "mixedNag": {
                 "type": "text",
                 "index": "false"
               }
             }
           },
           "paf": {
             "properties": {
               "buildingName": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "buildingNumber": {
                 "type": "short",
                 "index": "true"
               },
               "changeType": {
                 "type": "text",
                 "index": "false"
               },
               "deliveryPointSuffix": {
                 "type": "text",
                 "index": "false"
               },
               "departmentName": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer"
               },
               "dependentLocality": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "dependentThoroughfare": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "doubleDependentLocality": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "endDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "true",
                 "null_value": "2021-03-31T00:00:00Z"
               },
               "entryDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "false"
               },
               "lastUpdateDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "false"
               },
               "organisationName": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer"
               },
               "poBoxNumber": {
                 "type": "text",
                 "index": "false"
               },
               "postTown": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "postcode": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "postcodeType": {
                 "type": "text",
                 "index": "false"
               },
               "proOrder": {
                 "type": "long",
                 "index": "false"
               },
               "processDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "false"
               },
               "recordIdentifier": {
                 "type": "byte",
                 "index": "false"
               },
               "startDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "true"
               },
               "subBuildingName": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "thoroughfare": {
                 "type": "text",
                 "index": "true",
                 "analyzer": "keyword"
               },
               "udprn": {
                 "type": "integer",
                 "index": "false"
               },
               "uprn": {
                 "type": "long",
                 "index": "false"
               },
               "welshDependentLocality": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshDependentThoroughfare": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshDoubleDependentLocality": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshPostTown": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshThoroughfare": {
                 "type": "text",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "pafAll": {
                 "type": "text",
                 "analyzer": "welsh_split_analyzer",
                 "fields": {
                   "bigram": {
                     "type": "text",
                     "analyzer": "welsh_bigram_analyzer"
                   }
                 }
               },
               "mixedPaf": {
                 "type": "text",
                 "index": "false"
               },
               "mixedWelshPaf": {
                 "type": "text",
                 "index": "false"
               }
             }
           },
           "uprn": {
             "type": "long",
             "index": "true"
           },
           "postcodeIn": {
             "type": "text",
             "index": "true",
             "analyzer": "keyword"
           },
           "postcodeOut": {
             "type": "text",
             "index": "true",
             "analyzer": "keyword"
           },
           "parentUprn": {
             "type": "text",
             "index": "false"
           },
           "relatives": {
             "properties": {
               "level": {
                 "type": "integer",
                 "index": "false"
               },
               "siblings": {
                 "type": "long",
                 "index": "false"
               },
               "parents": {
                 "type": "long",
                 "index": "false"
               }
             }
           },
           "crossRefs": {
              "properties": {
                "crossReference": {
                  "type": "text",
                  "index": "false"
                },
                "source": {
                  "type": "text",
                  "index": "false"
                }
              }
           }
         }
       }
     }
   }
   """
}
