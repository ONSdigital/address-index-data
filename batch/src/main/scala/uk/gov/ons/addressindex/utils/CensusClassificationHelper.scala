package uk.gov.ons.addressindex.utils

object CensusClassificationHelper {

//  “RD” / “RD02” / “RD03” / “RD04” / “RD06” are ADDRESS_TYPE = “HH” and ESTAB_TYPE = “HOUSEHOLD”.
//  “RD08” is ADDRESS_TYPE = “HH” and ESTAB_TYPE = “SHELTERED ACCOMMODATION”.
//  “RH” / “RH01” / “RH02” / “RH03” are ADDRESS_TYPE = “HH” and ESTAB_TYPE = “HMO NOT SUB_DIVIDED”, “HMO SHELL” or “HMO UNIT” (in FLAT_FILEv8) but “HOUSEHOLD” in the Rehearsal Specification.
//  “RD01” / “RD07” / “RD10” / “CH02” are ADDRESS_TYPE = “HH” and ESTAB_TYPE = “HOUSEHOLD” if linked to Council Tax.
//  “RD01” / “RD10” are ADDRESS_TYPE = “SPG” and ESTAB_TYPE = “CARAVAN SITE” if not linked to Council Tax (in FLAT_FILEv8), but “Residential Caravanner” in the Rehearsal Specification.
//  “RD07” is ADDRESS_TYPE = “SPG” and ESTAB_TYPE = “MARINA” if not linked to Council Tax (in FLAT_FILEv8), but “Residential Boater” in the Rehearsal Specification.
//  “CH” / “CH01” / “CH01YH” / “CH03” are ADDRESS_TYPE = “CE” and ESTAB_TYPE = “HOTEL” if linked to Non-domestic rates.
//  “CH” / “CH01” / “CH01YH” / “CH03” are ADDRESS_TYPE = “HH” and ESTAB_TYPE = “HOUSEHOLD/GUEST HOUSE” if not linked to Non-domestic rates (in FLAT_FILEv8) but “HOUSEHOLD” in the Rehearsal Specification.
//
//  The other mapping that is not applied directly in FLAT_FILEv8 is for RI01 that is usually ADDRESS_TYPE = “CE” and ESTAB_TYPE = “CARE HOME”.

  def ABPToAddressType(classCode: String, councilTax: Boolean = true, nonDomesticRates: Boolean = false): String = classCode match {
    case "RD" | "RD02" | "RD03" | "RD04" | "RD06"  => "HH"
    case "RD08" => "HH"
    case "RH" | "RH01" | "RD02" | "RD03" => "HH"
    case "RD01" | "RD07" | "RD10" | "CH02"  if councilTax => "HH" // if council tax  7666VC
    case "RD01" | "RD07" | "RD10" | "CH02"  if !councilTax => "SPG" // if council tax  7666VC
    case "CH" | "CH01" | "CH01YH" if nonDomesticRates => "CE"    // if linked to Non-Domestic Rates 7666VN
    case "CH" | "CH01" | "CH01YH" if !nonDomesticRates => "HH"    // if not linked to Non-Domestic Rates 7666VN
    case "RI01" => "CE"
    case "RI03" => "CE"
    case _ => "NA"
  }

  def ABPToEstabType(classCode: String, councilTax: Boolean = true, nonDomesticRates: Boolean = false): String = classCode match {
    case "RD" | "RD02" | "RD03" | "RD04" | "RD06"  => "Household"
    case "RD08" => "Sheltered Accommodation"
    case "RH" | "RH01" | "RD02" | "RD03"  => "Household"
    case "RD01" | "RD07" | "RD10" | "CH02"  if councilTax => "Household" // if council tax  7666VC
    case "RD01" | "RD10" if !councilTax => "Residential Caravaner"  // if not council tax
    case "RD07" if !councilTax => "Residential Boater" // if not council tax  7666VC
    case "CH" | "CH01" | "CH01YH" if nonDomesticRates => "Hotel"    // if linked to Non-Domestic Rates 7666VN
    case "CH" | "CH01" | "CH01YH" if !nonDomesticRates => "Household"    // if not linked to Non-Domestic Rates 7666VN
    case "RI01" => "Care Home"
    case "RI03" => "Hall of Residence or Boarding School"
    case _ => "NA"
  }


}
