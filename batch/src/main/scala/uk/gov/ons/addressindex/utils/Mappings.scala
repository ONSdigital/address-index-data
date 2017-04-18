package uk.gov.ons.addressindex.utils

object Mappings {
  val hybrid =
    """
      {
        "settings": {
          "number_of_shards": 1,
          "number_of_replicas": 1,
          "analysis": {
            "filter": {
              "address_synonym_filter": {
                "type": "synonym",
                "synonyms": [

                  "AV, AV., AVE, AVE., AVEN, AVEN. => AVENUE",
                  "BDWY, BDWY. => BROADWAY",
                  "BLVD, BLVD. => BOULEVARD",
                  "CIR, CIR. => CIRCUS",
                  "CL, CL., CLS, CLS. => CLOSE",
                  "CT, CT., CRT, CRT. => COURT",
                  "CR, CR., CRES, CRES. => CRESCENT",
                  "DR, DR., DRV, DRV. => DRIVE",
                  "GDN, GRDN, GDN., GRDN. => GARDEN",
                  "GRDNS, GDNS => GARDENS",
                  "GN, GN. => GREEN",
                  "GR, GR. => GROVE",
                  "LN, LN., LNE, LNE. => LANE",
                  "MT, MT. => MOUNT",
                  "PL, PL. => PLACE",
                  "PK, PK. => PARK",
                  "QRY => QUARRY",
                  "RDG, RDG. => RIDGE",
                  "RD, RD. => ROAD",
                  "SQ, SQ. => SQUARE",
                  "STR, STR. => STREET",
                  "TER, TER. => TERRACE",
                  "VAL, VAL. => VALLEY",
                  "WLK => WALK",
                  "STREET => ST",
                  "SAINT => ST",
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
                  "GRD, GRND, GROUND",
                  "ANNEX, ANNEXE",
                  "APPT, APT, APARTMENT, FLT, FLAT, APPTS, APTS, APARTMENTS",
                  "BLD, BUILDING, BUILDINGS",
                  "NO, NUMBER",
                  "NOS, NUMBERS",
                  "HSE, HOUSE",
                  "BLK, BLOCK",
                  "FLR, FLOOR",
                  "ADJ, ADJACENT",
                  "LTD, LTD., LIMITED",
                  "CO, CO., COMPANY",
                  "ST., ST",
                  "CARE HOME, CAREHOME, RESIDENTIAL HOME, NURSING HOME, RETIREMENT HOME",
                  "HMP, HM PRISON",
                  "UNI, UNIV, UNIVERSITY",
                  "B&B, BED AND BREAKFAST",
                  "PUB, PH, PUBLIC HOUSE",
                  "FC, F.C., FOOTBALL CLUB",
                  "YHA, YOUTH HOSTEL ASSOCIATION",
                  "&, AND",
                  "CE, C OF E, COFE, CHURCH OF ENGLAND",
                  "RC, ROMAN CATHOLIC",
                  "URC, UNITED REFORM CHURCH, UNITED REFORMED CHURCH",
                  "GSM LONDON, GREENWICH SCHOOL OF MANAGEMENT",
                  "JUNIOR SCHOOL, INFANT SCHOOL, J&I SCHOOL, I SCHOOL, J SCHOOL, PRIMARY SCHOOL",
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
                  "VUE, VIEW",
                  "SPORTS CENTRE, LEISURE CENTRE",
                  "FARM, FARMHOUSE",
                  "BUSINESS PARK, INDUSTRIAL PARK",
                  "BUSINESS CENTRE, INDUSTRIAL CENTRE",
                  "DLR, DOCKLANDS LIGHT RAILWAY",
                  "POST OFFICE, DELIVERY OFFICE"

                ]
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
                  "crossReference": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "source": {
                    "type": "string",
                    "index": "not_analyzed"
                  },
                  "nagAll": {
                    "type": "string",
                    "analyzer": "welsh_split_analyzer"
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
                    "analyzer": "welsh_split_analyzer"
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
              }
            }
          }
        }
      }
    """
}
