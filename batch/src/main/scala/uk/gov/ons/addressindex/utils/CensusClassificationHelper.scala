package uk.gov.ons.addressindex.utils

object CensusClassificationHelper {

  def ABPToAddressType(classCode: String): String = classCode match {
      case "HH" => "HH"
      case "CE" => "CE"
      case "SPG" => "SPG"
      case _ => "NA"
  }

  def ABPToEstabType(classCode: String): String = classCode match {
    case "Household" => "Household"
    case "Hall of Residence" => "Hall of Residence"
    case "Care Home" => "Care Home"
    case "Boarding School" => "Boarding School"
    case "Hotel" => "Hotel"
    case "Hostel" => "Hostel"
    case "Sheltered Accommodation" => "Sheltered Accommodation"
    case "Hall of Residence" => "Hall of Residence"
    case "Residential Caravaner" => "Residential Caravaner"
    case "Gypsy Roma Traveller" => "Gypsy Roma Traveller"
    case "Residential Boater" => "Residential Boater"
    case _ => "NA"
  }


}
