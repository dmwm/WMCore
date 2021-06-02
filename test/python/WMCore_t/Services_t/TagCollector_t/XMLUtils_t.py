#!/usr/bin/env python
"""
Unittests for XMLUtils functions
"""

from __future__ import division, print_function

import unittest

from Utils.PythonVersion import PY3

from WMCore.Services.TagCollector.XMLUtils import xml_parser


class XMLUtilsTest(unittest.TestCase):
    """
    unittest for XMLUtils functions
    """

    def setUp(self):
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testParser(self):
        """
        Test parsing a basic XML as the one retrieved from TagCollector
        """
        data = '<projects>\n' \
               '  <architecture name="slc5_amd64_gcc462">\n' \
               '    <project label="CMSSW_5_3_6" type="Production" state="Announced"/>\n' \
               '    <project label="CMSSW_5_3_7" type="Production" state="Announced"/>\n' \
               '    <project label="CMSSW_5_3_8" type="Production" state="Announced"/>\n' \
               '  </architecture>\n' \
               '  <architecture name="slc7_ppc64le_gcc530">\n' \
               '    <project label="CMSSW_8_1_0_pre15" type="Development" state="Announced"/>\n' \
               '  </architecture>\n' \
               '  <architecture name="slc6_amd64_gcc620">\n' \
               '    <project label="CMSSW_9_0_0_pre3" type="Development" state="Announced"/>\n' \
               '    <project label="CMSSW_9_0_0_pre4" type="Development" state="Announced"/>\n' \
               '  </architecture>\n' \
               '</projects>\n'
        pkey = 'architecture'

        for row in xml_parser(data, pkey):
            self.assertEqual(type(row[pkey]['project']), list)
            print(row)
            if row[pkey]['name'] == 'slc5_amd64_gcc462':
                releases = [item['label'] for item in row[pkey]['project']]
                self.assertItemsEqual(['CMSSW_5_3_6', 'CMSSW_5_3_7', 'CMSSW_5_3_8'], releases)
            elif row[pkey]['name'] == 'slc7_ppc64le_gcc530':
                releases = [item['label'] for item in row[pkey]['project']]
                self.assertItemsEqual(['CMSSW_8_1_0_pre15'], releases)
            elif row[pkey]['name'] == 'slc6_amd64_gcc620':
                releases = [item['label'] for item in row[pkey]['project']]
                self.assertItemsEqual(['CMSSW_9_0_0_pre3', 'CMSSW_9_0_0_pre4'], releases)


if __name__ == '__main__':
    unittest.main()
