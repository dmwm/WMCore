#!/usr/bin/env python
"""
Unittests for IteratorTools functions
"""

from __future__ import division, print_function

import unittest

from WMCore.ReqMgr.DataStructs.RequestError import InvalidSpecParameterValue
from WMCore.ReqMgr.DataStructs.RequestStatus import ACTIVE_STATUS, get_modifiable_properties
from WMCore.ReqMgr.Utils.Validation import (validateOutputDatasets,
                                            validate_request_priority,
                                            _validate_request_allowed_args)
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


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


    def testValidateRequestAllowedArgs(self):
        """
        Tests the `_validate_request_allowed_args` functions, which validates two pairs
        of request arguments and returns the difference between them and on top of that
        applies a filter of allowed parameters changes per status
        :return: nothing, raises an exception if there are problems
        """
        defReqArgs = StdBase.getWorkloadAssignArgs()
        newReqArgs = {key: None for key in defReqArgs.keys()}

        for status in ACTIVE_STATUS:
            # NOTE: We need to add the RequestStatus artificially and assign it
            #       to the currently tested active status
            defReqArgs["RequestStatus"] = status
            expectedReqArgs = {key: None for key in get_modifiable_properties(status)}
            reqArgsDiff = _validate_request_allowed_args(defReqArgs, newReqArgs)
            print(f"reqArgsDiff: {reqArgsDiff}")
            print(f"expectedReqArgs: {expectedReqArgs}")
            self.assertDictEqual(reqArgsDiff, expectedReqArgs)
            print("===============================")


if __name__ == '__main__':
    unittest.main()
