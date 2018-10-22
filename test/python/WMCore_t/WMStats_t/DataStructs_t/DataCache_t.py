#!/usr/bin/python
"""
_DataCache_t_

General test of Lexicon

"""

import unittest
import os
import json
import pdb
from pprint import pprint
from WMCore.WMStats.DataStructs.DataCache import wmstatsFilter
from WMCore.WMBase import getTestBase

testJSON = os.path.join(getTestBase(), "WMCore_t/WMStats_t/DataStructs_t/TestData.json" )

with open(testJSON, 'r') as f:
    TEST_DATA = json.load(f)

class DataCacheTest(unittest.TestCase):

    def testWMStatsFilter(self):
        result = {}
        for request, data in TEST_DATA.items():
            result[request] = wmstatsFilter(data)

        pprint(result)

if __name__ == "__main__":
    unittest.main()