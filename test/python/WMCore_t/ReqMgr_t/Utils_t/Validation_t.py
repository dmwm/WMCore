#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""

from __future__ import division, print_function

import unittest

from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue
from WMCore.ReqMgr.Utils.Validation import validateOutputDatasets, validate_request_priority


class ValidationTests(unittest.TestCase):
    """
    unittest for ReqMgr Utils Validation functions
    """

    def testValidateOutputDatasets(self):
        """
        Test the validateOutputDatasets function
        """
        dbsUrl = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader/'

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

    def testRequestPriorityValidation(self):
        """
        Test the `validate_request_priority` function, which validates the
        RequestPriority parameter
        :return: nothing, raises an exception if there are problems
        """
        # test valid cases, integer in the range of [0, 1e6]
        for goodPrio in [0, 100, int(1e6 - 1)]:
            reqArgs = {'RequestPriority': goodPrio}
            print(reqArgs)
            validate_request_priority(reqArgs)

        # test invalid ranges
        for badPrio in [-10, 1e6, 1e7]:
            reqArgs = {'RequestPriority': badPrio}
            with self.assertRaises(InvalidSpecParameterValue):
                validate_request_priority(reqArgs)

        # test invalid data types
        for badPrio in ["1234", 1234.35, 1e6, [123]]:
            reqArgs = {'RequestPriority': badPrio}
            with self.assertRaises(InvalidSpecParameterValue):
                validate_request_priority(reqArgs)


if __name__ == '__main__':
    unittest.main()
