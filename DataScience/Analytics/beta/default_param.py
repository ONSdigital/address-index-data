# -*- coding: utf-8 -*-
"""
Created on Mon Jul  3 08:08:17 2017

@author: spakui
"""

DEFAULT_CONFIG = { 
              "subBuildingName":{  
                 "lpiSaoStartNumberBoost":1.0,
                 "lpiSaoStartSuffixBoost":1.0,
                 "lpiSaoEndNumberBoost":1.0,
                 "lpiSaoEndSuffixBoost":1.0,
                 "pafSubBuildingNameBoost":1.5,
                 "lpiSaoTextBoost":1.5
              },
              "buildingName":{  
                 "lpiPaoStartNumberBoost":2.5,
                 "lpiPaoStartSuffixBoost":3.5,
                 "lpiPaoEndNumberBoost":2.5,
                 "lpiPaoEndSuffixBoost":2.5,
                 "pafBuildingNameBoost":2.5,
                 "lpiPaoTextBoost":2.5
              },
              "buildingNumber":{  
                 "pafBuildingNumberBoost":2.0,
                 "lpiPaoStartNumberBoost":2.0
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
                 "postcodeInOutBoost":0.8,
                 "postcodeOutBoost":0.8,
                 "postcodeInBoost":0.3
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
                 "pafDependentLocalityBoost":0.5,
                 "pafWelshDependentLocalityBoost":0.5,
                 "lpiLocalityBoost":0.5,
                 "pafDoubleDependentLocalityBoost":0.3,
                 "pafWelshDoubleDependentLocalityBoost":0.3
              },
              "excludingDisMaxTieBreaker":0.0,
              "includingDisMaxTieBreaker":0.5,
              "paoSaoMinimumShouldMatch": "-45%",
              "organisationDepartmentMinimumShouldMatch": "30%",
              "fallbackQueryBoost":0.2,
              "defaultBoost":1.0,
              "mainMinimumShouldMatch": "-35%",
              "fallbackMinimumShouldMatch": "-40%",
              #"nagQueryBoost": 2,
              #"pafQueryBoost": 1,
              "fallbackPafBoost": 1.0,
              "fallbackLpiBoost": 1.0,
              "fallbackPafBigramBoost": 0.2,
              "fallbackLpiBigramBoost": 0.2,
              "bigramFuzziness": "0"
           }