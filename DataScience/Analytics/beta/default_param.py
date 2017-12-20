# -*- coding: utf-8 -*-
"""
Created on Mon Jul  3 08:08:17 2017

@author: spakui
"""

DEFAULT_CONFIG = { 
              "subBuildingName":{
                 "pafSubBuildingNameBoost":1.5,
                 "lpiSaoTextBoost":1.5,
                 "lpiSaoStartNumberBoost":1.0,
                 "lpiSaoStartSuffixBoost":1.0
              },
              "subBuildingRange":{  
                 "lpiSaoStartNumberBoost":1.0,
                 "lpiSaoStartSuffixBoost":1.0,
                 "lpiSaoEndNumberBoost":1.0,
                 "lpiSaoEndSuffixBoost":1.0,
                 "lpiSaoStartEndBoost":0.1
              },
              "buildingName":{  
                 "lpiPaoStartSuffixBoost":3.0,
                 "pafBuildingNameBoost":2.5,
                 "lpiPaoTextBoost":2.5
              },
              "buildingNumber":{  
                 "pafBuildingNumberBoost":3.0, #lower because they sometimes put the sao number there 
                 "lpiPaoStartNumberBoost":3.5,
                 "lpiPaoEndNumberBoost":0.1                 
              },
              "buildingRange":{  
                 "lpiPaoStartNumberBoost":2.0,
                 "lpiPaoStartSuffixBoost":2.0,
                 "lpiPaoEndNumberBoost":2.0,
                 "lpiPaoEndSuffixBoost":2.0,
                 "lpiPaoStartEndBoost":0.1,
                 "pafBuildingNumberBoost":0.1
              },
              "streetName":{  
                 "pafThoroughfareBoost":2.0,
                 "pafWelshThoroughfareBoost":2.0,
                 "pafDependentThoroughfareBoost":0.5,
                 "pafWelshDependentThoroughfareBoost":0.5,
                 "lpiStreetDescriptorBoost":2.0
              },
              "townName":{  
                 "pafPostTownBoost":1.0,
                 "pafWelshPostTownBoost":1.0,
                 "lpiTownNameBoost":1.0,
                 "pafDependentLocalityBoost":0.5,
                 "pafWelshDependentLocalityBoost":0.5,
                 "lpiLocalityBoost":0.5,
                 "pafDoubleDependentLocalityBoost":0.2,
                 "pafWelshDoubleDependentLocalityBoost":0.2
              },
              "postcode":{  
                 "pafPostcodeBoost":1.0,
                 "lpiPostcodeLocatorBoost":1.0,
                 "postcodeInOutBoost":0.5
              },
              "organisationName":{  
                 "pafOrganisationNameBoost":1.0,
                 "lpiOrganisationBoost":1.0,
                 "lpiPaoTextBoost":1.0,
                 "lpiLegalNameBoost":1.0,
                 "lpiSaoTextBoost":0.5
              },
              "departmentName":{  
                 "pafDepartmentNameBoost":1.0,
                 "lpiLegalNameBoost":0.5
              },
              "locality":{  
                 "pafPostTownBoost":0.2,
                 "pafWelshPostTownBoost":0.2,
                 "lpiTownNameBoost":0.2,
                 "pafDependentLocalityBoost":0.6,
                 "pafWelshDependentLocalityBoost":0.6,
                 "lpiLocalityBoost":0.6,
                 "pafDoubleDependentLocalityBoost":0.3,
                 "pafWelshDoubleDependentLocalityBoost":0.3
              },
              "fallback":{
                "fallbackQueryBoost":0.075,
                "fallbackPafBoost": 1.0,
                "fallbackLpiBoost": 1.0,
                "fallbackPafBigramBoost": 0.2,
                "fallbackLpiBigramBoost": 0.2,
                "fallbackMinimumShouldMatch": "-40%",
                "bigramFuzziness": "0"
              },
              "excludingDisMaxTieBreaker":0.0,
              "includingDisMaxTieBreaker":0.5,
              "topDisMaxTieBreaker":1.0,
              "paoSaoMinimumShouldMatch": "-45%",
              "organisationDepartmentMinimumShouldMatch": "30%",             
              "mainMinimumShouldMatch": "-40%"
           }