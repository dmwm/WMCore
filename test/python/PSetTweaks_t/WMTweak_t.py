#!/usr/bin/env python
"""
Unittest for WMTweak module

"""


import unittest
import PSetTweaks.WMTweak as WMTweaks


class WMTweakTest(unittest.TestCase):


    def testA(self):
        """instantiation"""

        try:

            tweak = WMTweaks.TweakMaker()
        except Exception, ex:
            msg = "Failed to instantiate WMTweak:\n"
            msg += str(ex)
            self.fail(msg)


if __name__ == '__main__':
    unittest.main()






