"""
ONS Address Index - Probabilistic Parser Test
=============================================

A few unit tests to check that the parser performs as expected.


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 7-Feb-2017
"""
import unittest
from collections import OrderedDict

from ProbabilisticParser import parser


class TestParser(unittest.TestCase):
    def test_only_building_number(self):
        assert parser.parse('1') == [('1', 'BuildingNumber')]
        assert parser.parse('232') == [('232', 'BuildingNumber')]
        assert parser.parse('256') == [('256', 'BuildingNumber')]
        assert parser.parse('1234') == [('1234', 'BuildingNumber')]

    def test_only_sub_building_name(self):
        assert parser.tag('Flat 5') == OrderedDict([('SubBuildingName', 'Flat 5')])
        assert parser.tag('Apartment 1C') == OrderedDict([('SubBuildingName', 'Apartment 1C')])
        assert parser.tag('Unit A') == OrderedDict([('SubBuildingName', 'Unit A')])

    def test_only_building_name(self):
        assert parser.tag('5C') == OrderedDict([('BuildingName', '5C')])
        assert parser.tag('Victorian House') == OrderedDict([('BuildingName', 'Victorian House')])
        assert parser.tag('SHAKESPEARE HOUSE') == OrderedDict([('BuildingName', 'SHAKESPEARE HOUSE')])

    def test_only_street_name(self):
        assert parser.tag('Oxford Road') == OrderedDict([('StreetName', 'Oxford Road')])
        assert parser.tag('Regent Street') == OrderedDict([('StreetName', 'Regent Street')])
        assert parser.tag('NORFOLK DRIVE') == OrderedDict([('StreetName', 'NORFOLK DRIVE')])
        assert parser.tag('LONDON ROAD') == OrderedDict([('StreetName', 'LONDON ROAD')])
        assert parser.tag('ST. JAMES STREET') == OrderedDict([('StreetName', 'ST. JAMES STREET')])

    def test_only_postcode(self):
        assert parser.parse('RH1 2FW') == [('RH1', 'Postcode'), ('2FW', 'Postcode')]
        assert parser.parse('RH12FW') == [('RH12FW', 'Postcode')]
        assert parser.parse('L1 1XX') == [('L1', 'Postcode'), ('1XX', 'Postcode')]
        assert parser.parse('KT18') == [('KT18', 'Postcode')]
        assert parser.parse('SW1P') == [('SW1P', 'Postcode')]
        assert parser.tag('WF11 9ZZ') == OrderedDict([('Postcode', 'WF11 9ZZ')])
        assert parser.tag('EC1N 8QX') == OrderedDict([('Postcode', 'EC1N 8QX')])
        assert parser.tag('EC1N8QX') == OrderedDict([('Postcode', 'EC1N8QX')])
        assert parser.tag('SY23 3SR') == OrderedDict([('Postcode', 'SY23 3SR')])
        assert parser.tag('SY233SR') == OrderedDict([('Postcode', 'SY233SR')])

    def test_only_organisation(self):
        assert parser.parse('Statistics Ltd') == [('Statistics', 'OrganisationName'), ('Ltd', 'OrganisationName')]
        assert parser.tag('THE GLENSIDE HOSPITAL FOR NEURO REHABILITATION') == \
               OrderedDict([('OrganisationName', 'THE GLENSIDE HOSPITAL FOR NEURO REHABILITATION')])

    def test_only_town(self):
        assert parser.parse('Oxford') == [('Oxford', 'TownName')]

    def test_addresses(self):
        assert parser.tag('FLAT 1 7 DENZIL AVENUE SOUTHAMPTON') == OrderedDict([('SubBuildingName', 'FLAT 1'),
                                                                                ('BuildingNumber', '7'),
                                                                                ('StreetName', 'DENZIL AVENUE'),
                                                                                ('TownName', 'SOUTHAMPTON')])
        assert parser.tag('NIGHTINGALES RESIDENTIAL HOME WOLVERLEY COURT WOLVERLEY ' +
                          'ROAD WOLVERLEY KIDDERMINSTER DY10 3RP') == \
               OrderedDict([('OrganisationName', 'NIGHTINGALES RESIDENTIAL HOME'),
                            ('BuildingName', 'WOLVERLEY COURT'),
                            ('StreetName', 'WOLVERLEY ROAD'),
                            ('Locality', 'WOLVERLEY'),
                            ('TownName', 'KIDDERMINSTER'),
                            ('Postcode', 'DY10 3RP')])
        assert parser.tag('12 ST ALBANS ROAD WATFORD WD17 1UN') == OrderedDict([('BuildingNumber', '12'),
                                                                                ('StreetName', 'ST ALBANS ROAD'),
                                                                                ('TownName', 'WATFORD'),
                                                                                ('Postcode', 'WD17 1UN')])
        assert parser.tag('FLAT 30 68 VINCENT SQUARE LONDON SW1P 2NZ') == OrderedDict([('SubBuildingName', 'FLAT 30'),
                                                                                       ('BuildingNumber', '68'),
                                                                                       ('StreetName', 'VINCENT SQUARE'),
                                                                                       ('TownName', 'LONDON'),
                                                                                       ('Postcode', 'SW1P 2NZ')])
        assert parser.tag('FLAT 4.5.3 LIBERTY QUAYS BLAKE AVENUE GILLINGHAM') == \
               OrderedDict([('SubBuildingName', 'FLAT 4.5.3'),
                            ('BuildingName', 'LIBERTY QUAYS'),
                            ('StreetName', 'BLAKE AVENUE'),
                            ('TownName', 'GILLINGHAM')])

        assert parser.tag('STUDIO 1.2 BLOCK J BIRKS HALLS NEW NORTH ROAD EXETER EX4 4ZZ') == \
               OrderedDict([('SubBuildingName', 'STUDIO 1.2 BLOCK J'),
                            ('BuildingName', 'BIRKS HALLS'),
                            ('StreetName', 'NEW NORTH ROAD'),
                            ('TownName', 'EXETER'),
                            ('Postcode', 'EX4 4ZZ')])
        assert parser.tag('FLAT 50 BECK MILL COURT BECK MILL STREET MELTON MOWBRAY LE13 1PT') == \
               OrderedDict([('SubBuildingName', 'FLAT 50'),
                            ('BuildingName', 'BECK MILL COURT'),
                            ('StreetName', 'BECK MILL STREET'),
                            ('TownName', 'MELTON MOWBRAY'),
                            ('Postcode', 'LE13 1PT')])


if __name__ == '__main__':
    unittest.main(verbosity=3)
