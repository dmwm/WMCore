#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""

from __future__ import division, print_function

import unittest

from WMCore.ReqMgr.Utils.Validation import validateOutputDatasets
from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue


class ValidationTests(unittest.TestCase):
    """
    unittest for ReqMgr Utils Validation functions
    """

    def testValidateOutputDatasets(self):
        """
        Test the validateOutputDatasets function
        """
        dbsUrl = 'https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader/'

        outputDsets = ['/PD1/AcqEra1-ProcStr1-v1/GEN']
        self.assertIsNone(validateOutputDatasets(outputDsets, dbsUrl))
        outputDsets.append('/PD1/AcqEra1-ProcStr1-v1/GEN-SIM')
        self.assertIsNone(validateOutputDatasets(outputDsets, dbsUrl))
        outputDsets.append('/PD1/AcqEra1-ProcStr1-v1/GEN-SIM-RAW')
        self.assertIsNone(validateOutputDatasets(outputDsets, dbsUrl))

        outputDsets.append('/PD1/AcqEra1-ProcStr1-v1/GEN')
        with self.assertRaises(InvalidSpecParameterValue):
            validateOutputDatasets(outputDsets, dbsUrl)

        outputDsets.remove('/PD1/AcqEra1-ProcStr1-v1/GEN')
        outputDsets.append('/PD1//AOD')
        with self.assertRaises(InvalidSpecParameterValue):
            validateOutputDatasets(outputDsets, dbsUrl)

        outputDsets.remove('/PD1//AOD')
        outputDsets.append('/PD1/None/AOD')
        with self.assertRaises(InvalidSpecParameterValue):
            validateOutputDatasets(outputDsets, dbsUrl)

        outputDsets.remove('/PD1/None/AOD')
        outputDsets.append('/PD1/AcqEra1-ProcStr1-v1/ALAN')
        with self.assertRaises(InvalidSpecParameterValue):
            validateOutputDatasets(outputDsets, dbsUrl)


if __name__ == '__main__':
    unittest.main()
