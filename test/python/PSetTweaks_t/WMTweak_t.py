#!/usr/bin/env python
"""
Unittest for WMTweak module

"""

import unittest

import PSetTweaks.WMTweak as WMTweaks
from PSetTweaks.WMTweak import WMTweakMaskError
from PSetTweaks.PSetTweak import PSetTweak

from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.Job import Job

class WMTweakTest(unittest.TestCase):


    def testInit(self):
        """
        _testInit_
        Tests the tweakMaker initialization
        """

        try:
            tweak = WMTweaks.TweakMaker()
        except Exception as ex:
            msg = "Failed to instantiate WMTweak:\n"
            msg += str(ex)
            self.fail(msg)

    def testFirstEvent(self):
        """
        _testFirstEvent_

        Verify that we set the firstEvent parameter whenever the FirstEvent
        field in the job mask is a positive integer. And the job is
        not production.
        """
        job = Job()
        job["input_files"] = [{"lfn": "bogusFile", "parents": []}]
        job["mask"] = Mask()

        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertFalse(hasattr(tweak.process.source, "skipEvents"),
                         "Error: There should be no skipEvents tweak.")
        self.assertFalse(hasattr(tweak.process.source, "firstEvent"),
                         "Error: There should be no firstEvent tweak.")

        job["mask"]["FirstEvent"] = 0
        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertTrue(hasattr(tweak.process.source, "skipEvents"),
                        "Error: There should be a skipEvents tweak.")
        self.assertEqual(tweak.process.source.skipEvents, 'customTypeCms.untracked.uint32(0)',
                         "Error: The skipEvents tweak should be 0.")
        return

    def testFirstRun(self):
        """
        _testFirstRun_
        Verify that when we set the FirstRun in the mask, it is set in the
        process but when it is not, then no firstRun appears in the process.
        This for jobs with real input files.
        """
        job = Job()
        job["input_files"] = [{"lfn": "bogusFile", "parents": []}]
        job["mask"] = Mask()

        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertFalse(hasattr(tweak.process.source, "firstRun"),
                         "Error: There should be no firstRun tweak.")

        job["mask"]["FirstRun"] = 93
        tweak = WMTweaks.makeJobTweak(job, tweak)

        self.assertTrue(hasattr(tweak.process.source, "firstRun"),
                        "Error: There should be a firstRun tweak.")
        self.assertEqual(tweak.process.source.firstRun, 'customTypeCms.untracked.uint32(93)',
                         "Error: The firstRun tweak should be 93.")
        return

    def testFirstEventMC(self):
        """
        _testFirstEventMC_

        Verify that we set the firstEvent parameter whenever the FirstEvent
        field in the job mask is a positive integer and the job is a production
        one. Otherwise we get a number based on the counter (modulo 2^32 - 1)
        """
        job = Job()
        job["input_files"] = [{"lfn": "MCFakeFile", "parents": []}]
        job["mask"] = Mask()
        job["counter"] = 5
        job["mask"]["FirstLumi"] = 200

        try:
            tweak = PSetTweak()
            WMTweaks.makeJobTweak(job, tweak)
            self.assertRaises(WMTweakMaskError,  WMTweaks.makeJobTweak, job)
        except WMTweakMaskError:
            pass

        job["mask"]["FirstEvent"] = 100
        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)
        self.assertFalse(hasattr(tweak.process.source, "skipEvents"),
                        "Error: There should be no skipEvents tweak, it's MC.")
        self.assertTrue(hasattr(tweak.process.source, "firstEvent"),
                        "Error: There should be a first event tweak")
        self.assertEqual(tweak.process.source.firstEvent, 'customTypeCms.untracked.uint32(100)',
                         "Error: The firstEvent tweak should be 100.")
        return

    def testFirstLumiMC(self):
        """
        _testFirstLumiMC_
        Verify that we set the lumi in a MC job and it gets into
        process.source.firstRun parameter, and if we don't at least we
        get the counter there.
        """
        job = Job()
        job["input_files"] = [{"lfn": "MCFakeFile", "parents": []}]
        job["mask"] = Mask()
        job["counter"] = 5
        job["mask"]["FirstEvent"] = 100

        try:
            tweak = PSetTweak()
            WMTweaks.makeJobTweak(job, tweak)
            self.assertRaises(WMTweakMaskError,  WMTweaks.makeJobTweak, job)
        except WMTweakMaskError:
            pass

        job["mask"]["FirstLumi"] = 200
        tweak =  PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertTrue(hasattr(tweak.process.source, "firstLuminosityBlock"),
                        "Error: There should be a first lumi tweak")
        self.assertEqual(tweak.process.source.firstLuminosityBlock, 'customTypeCms.untracked.uint32(200)',
                       "Error: The first luminosity block should be 200")

        job["mask"]["FirstLumi"] = 10
        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertTrue(hasattr(tweak.process.source, "firstLuminosityBlock"),
                        "Error: There should be a first lumi tweak")
        self.assertEqual(tweak.process.source.firstLuminosityBlock, 'customTypeCms.untracked.uint32(10)',
                       "Error: The first luminosity block should be 10")

    def testFirstRunMC(self):
        """
        _testFirstRunMC_
        Verify that we set the lumi in a MC job and it gets into
        process.source.firstRun parameter.
        """
        job = Job()
        job["input_files"] = [{"lfn": "MCFakeFile", "parents": []}]
        job["mask"] = Mask()
        job["mask"]["FirstLumi"] = 200
        job["mask"]["FirstEvent"] = 100
        job["counter"] = 5

        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertTrue(hasattr(tweak.process.source, "firstRun"),
                        "Error: There should be a first run tweak")
        self.assertEqual(tweak.process.source.firstRun, 'customTypeCms.untracked.uint32(1)',
                       "Error: The first run should be 1")

        job["mask"]["FirstRun"] = 5
        tweak = PSetTweak()
        WMTweaks.makeJobTweak(job, tweak)

        self.assertTrue(hasattr(tweak.process.source, "firstRun"),
                        "Error: There should be a first run tweak")
        self.assertEqual(tweak.process.source.firstRun, 'customTypeCms.untracked.uint32(5)',
                       "Error: The first run should be 5")
if __name__ == '__main__':
    unittest.main()
