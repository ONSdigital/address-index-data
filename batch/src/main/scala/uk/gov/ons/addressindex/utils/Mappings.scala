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
               "AV, AVE, AVEN => AVENUE",
               "BDWY => BROADWAY",
               "BLVD => BOULEVARD",
               "CIR => CIRCUS",
               "CL, CLS => CLOSE",
               "CT, CRT => COURT",
               "CR, CRES => CRESCENT",
               "DR, DRV => DRIVE",
               "GDN, GRDN => GARDEN",
               "GRDNS, GDNS => GARDENS",
               "GN => GREEN",
               "GR => GROVE",
               "LN, LNE => LANE",
               "MT => MOUNT",
               "PL => PLACE",
               "PK => PARK",
               "QRY => QUARRY",
               "RDG => RIDGE",
               "RD => ROAD",
               "SQ => SQUARE",
               "STR => STREET",
               "TER => TERRACE",
               "VAL => VALLEY",
               "WLK => WALK",
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
               "APPT, APT, APARTMENT, FLT, FLAT",
               "APPTS, APTS, APARTMENTS",
               "BLD, BUILDING, BUILDINGS",
               "NO, NUMBER",
               "NOS, NUMBERS",
               "HSE => HOUSE",
               "BLK => BLOCK",
               "FLR => FLOOR",
               "ADJ, ADJACENT",
               "LTD, LIMITED",
               "CO, COMPANY",
               "UNI, UNIV, UNIVERSITY",
               "B&B => BREAKFAST",
               "PUB, PH => PUBLIC",
               "FC, F.C, FOOTBALL",
               "CE, URC => CHURCH",
               "RC => CATHOLIC",
               "YSGOL, SCHOOL",
               "SPORTS, LEISURE",
               "FARM, FARMHOUSE",
               "BUSINESS, INDUSTRIAL",
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
           }
         }
       }
     }
   }
   """
}