package uk.gov.ons.addressindex.utils


object Mappings {
  val hybrid =
  """
   {
     "settings": {
       "number_of_shards": 1,
       "number_of_replicas": 3,
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
           }
         },
         "tokenizer": {
           "custom_keyword": {
             "type": "keyword",
             "buffer_size": 128
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
                 "type": "string",
                 "index": "not_analyzed"
               },
               "classificationCode": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "easting": {
                 "type": "float",
                 "index": "not_analyzed"
               },
               "location": {
                 "type": "geo_point",
                 "index": "not_analyzed"
               },
               "legalName": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer"
               },
               "level": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "locality": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "lpiLogicalStatus": {
                 "type": "byte",
                 "index": "not_analyzed"
               },
               "blpuLogicalStatus": {
                 "type": "byte",
                 "index": "not_analyzed"
               },
               "lpiKey": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "northing": {
                 "type": "float",
                 "index": "not_analyzed"
               },
               "officialFlag": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "organisation": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer"
               },
               "paoEndNumber": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "paoEndSuffix": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "paoStartNumber": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "paoStartSuffix": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "paoText": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer"
               },
               "postcodeLocator": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "saoEndNumber": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "saoEndSuffix": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "saoStartNumber": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "saoStartSuffix": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "saoText": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer"
               },
               "streetDescriptor": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "townName": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "uprn": {
                 "type": "long",
                 "index": "not_analyzed"
               },
               "usrn": {
                 "type": "integer",
                 "index": "not_analyzed"
               },
               "parentUprn": {
                 "type": "long",
                 "index": "not_analyzed"
               },
               "multiOccCount": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "localCustodianCode": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "rpc": {
                 "type": "byte",
                 "index": "not_analyzed"
               },
               "usrnMatchIndicator": {
                 "type": "byte",
                 "index": "not_analyzed"
               },
               "language": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "streetClassification": {
                 "type": "byte",
                 "index": "not_analyzed"
               },
               "classScheme": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "nagAll": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer",
                 "fields": {
                   "bigram": {
                     "type": "string",
                     "analyzer": "welsh_bigram_analyzer"
                   }
                 }
               },
               "lpiStartDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "lpiLastUpdateDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "lpiEndDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               }
             }
           },
           "paf": {
             "properties": {
               "buildingName": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "buildingNumber": {
                 "type": "short",
                 "index": "not_analyzed"
               },
               "changeType": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "deliveryPointSuffix": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "departmentName": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer"
               },
               "dependentLocality": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "dependentThoroughfare": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "doubleDependentLocality": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "endDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "entryDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "lastUpdateDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "organisationName": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer"
               },
               "poBoxNumber": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "postTown": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "postcode": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "postcodeType": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "proOrder": {
                 "type": "long",
                 "index": "not_analyzed"
               },
               "processDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "recordIdentifier": {
                 "type": "byte",
                 "index": "not_analyzed"
               },
               "startDate": {
                 "type": "date",
                 "format": "strict_date_optional_time||epoch_millis",
                 "index": "not_analyzed"
               },
               "subBuildingName": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "thoroughfare": {
                 "type": "string",
                 "index": "not_analyzed"
               },
               "udprn": {
                 "type": "integer",
                 "index": "not_analyzed"
               },
               "uprn": {
                 "type": "long",
                 "index": "not_analyzed"
               },
               "welshDependentLocality": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshDependentThoroughfare": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshDoubleDependentLocality": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshPostTown": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "welshThoroughfare": {
                 "type": "string",
                 "analyzer": "welsh_no_split_analyzer"
               },
               "pafAll": {
                 "type": "string",
                 "analyzer": "welsh_split_analyzer",
                 "fields": {
                   "bigram": {
                     "type": "string",
                     "analyzer": "welsh_bigram_analyzer"
                   }
                 }
               }
             }
           },
           "uprn": {
             "type": "long",
             "index": "not_analyzed"
           },
           "postcodeIn": {
             "type": "string",
             "index": "not_analyzed"
           },
           "postcodeOut": {
             "type": "string",
             "index": "not_analyzed"
           },
           "parentUprn": {
             "type": "string",
             "index": "not_analyzed"
           },
           "relatives": {
             "properties": {
               "level": {
                 "type": "integer",
                 "index": "not_analyzed"
               },
               "siblings": {
                 "type": "long",
                 "index": "not_analyzed"
               },
               "parents": {
                 "type": "long",
                 "index": "not_analyzed"
               }
             }
           },
           "crossRefs": {
              "properties": {
                "uprn": {
                  "type": "long",
                  "index": "not_analyzed"
                },
                "crossReference": {
                  "type": "string",
                  "index": "not_analyzed"
                },
                "source": {
                  "type": "string",
                  "index": "not_analyzed"
                }
              }
           }
         }
       }
     }
   }
   """
}
