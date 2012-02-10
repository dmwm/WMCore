#!/usr/bin/env python
"""
Unittest for WMTweak module

"""

import unittest

import PSetTweaks.WMTweak as WMTweaks

from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.Job import Job

class WMTweakTest(unittest.TestCase):


    def testA(self):
        """instantiation"""

        try:

            tweak = WMTweaks.TweakMaker()
        except Exception, ex:
            msg = "Failed to instantiate WMTweak:\n"
            msg += str(ex)
            self.fail(msg)

    def testFirstEvent(self):
        """
        _testFirstEvent_

        Verify that we set the firstEvent parameter whenever the FirstEvent
        field in the job mask is a positive integer.
        """
        job = Job()
        job["input_files"] = [{"lfn": "bogusFile", "parents": []}]
        job["mask"] = Mask()

        tweak = WMTweaks.makeJobTweak(job)

        self.assertFalse(hasattr(tweak.process.source, "skipEvents"),
                         "Error: There should be no skipEvents tweak.")

        job["mask"]["FirstEvent"] = 0
        tweak = WMTweaks.makeJobTweak(job)

        self.assertTrue(hasattr(tweak.process.source, "skipEvents"),
                        "Error: There should be no skipEvents tweak.")
        self.assertEqual(tweak.process.source.skipEvents, 0,
                         "Error: The skipEvents tweak should be 0.")
        return
    


if __name__ == '__main__':
    unittest.main()






