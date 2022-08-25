#!/usr/bin/env python
"""
Unittests for the Services/Rucio/RucioUtils module
"""
import logging
import unittest

from WMCore.Services.Rucio.RucioUtils import (RUCIO_VALID_PROJECT, validateMetaData,
                                              isTapeRSE, dropTapeRSEs, weightedChoice)


class RucioUtilsTest(unittest.TestCase):
    """
    Test class for the RucioUtils module
    """

    def setUp(self):
        """
        Setup for unit tests
        """
        self.logger = logging.getLogger()

    def tearDown(self):
        """
        Nothing to be done for this case
        """

    def testMetaDataValidation(self):
        """
        Test the `validateMetaData` validation function
        """
        for thisProj in RUCIO_VALID_PROJECT:
            response = validateMetaData("any_DID_name", dict(project=thisProj), self.logger)
            self.assertTrue(response)

        # test with no "project" meta data at all
        response = validateMetaData("any_DID_name", {}, self.logger)
        self.assertTrue(response)

        # now an invalid "project" meta data
        response = validateMetaData("any_DID_name", {"project": "mistake"}, self.logger)
        self.assertFalse(response)

    def testWeightedChoice(self):
        """
        Test the `weightedChoice` utilitarian function
        """
        pickDistr = {"CERN": 0, "FNAL": 0, "KIT": 0}
        rsesWithApproval = []
        rsesWeight = []
        for idx, rseName in enumerate(pickDistr):
            rsesWithApproval.append((rseName, False))
            rsesWeight.append((idx * 10) + 10)

        for _ in range(11):
            rse = weightedChoice(rsesWithApproval, rsesWeight)[0]
            pickDistr[rse] += 1
        # it's not a reproducible distribution, so just assume
        # that CERN is barely selected
        self.assertTrue(pickDistr["CERN"] <= 2)

    def testIsTapeRSE(self):
        """
        Test the `isTapeRSE` utilitarian function
        """
        self.assertTrue(isTapeRSE("T1_US_FNAL_Tape"))
        self.assertFalse(isTapeRSE("T1_US_FNAL_Disk"))
        self.assertFalse(isTapeRSE("T1_US_FNAL_Disk_Test"))
        self.assertFalse(isTapeRSE("T1_US_FNAL_Tape_Test"))
        self.assertFalse(isTapeRSE(""))

    def testDropTapeRSEs(self):
        """
        Test the `dropTapeRSEs` utilitarian function
        """
        tapeOnly = ["T1_US_FNAL_Tape", "T1_ES_PIC_Tape"]
        diskOnly = ["T1_US_FNAL_Disk", "T1_US_FNAL_Disk_Test", "T2_CH_CERN"]
        mixed = ["T1_US_FNAL_Tape", "T1_US_FNAL_Disk", "T1_US_FNAL_Disk_Test", "T1_ES_PIC_Tape"]
        self.assertCountEqual(dropTapeRSEs(tapeOnly), [])
        self.assertCountEqual(dropTapeRSEs(diskOnly), diskOnly)
        self.assertCountEqual(dropTapeRSEs(mixed), ["T1_US_FNAL_Disk", "T1_US_FNAL_Disk_Test"])


if __name__ == '__main__':
    unittest.main()
