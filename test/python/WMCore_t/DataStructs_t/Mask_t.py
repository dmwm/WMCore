#!/usr/bin/env python
"""
_Mask_

Unittest for the WMCore.DataStructs.Mask class

"""


# This code written as essentially a blank for future
# Mask development
# -mnorman


import unittest
from WMCore.DataStructs.Mask import Mask


class MaskTest(unittest.TestCase):
    """
    _MaskTest_

    """



    def testSetMaxAndSkipEvents(self):
        """
        test class for setMaxAndSkipEvents in Mask.py

        """

        print "testSetMaxAndSkipEvents"

        testMask = Mask()
        maxEvents  = 100
        skipEvents = 10

        testMask.setMaxAndSkipEvents(maxEvents, skipEvents)

        self.assertEqual(testMask['FirstEvent'], skipEvents)
        self.assertEqual(testMask['LastEvent'],  maxEvents + skipEvents)

        return

    def testGetMaxEvents(self):
        """
        test class for getMaxEvents in Mask.py

        """

        #The way I've decided to implement this depends on SetMaxAndSkipEvents()
        #Therefore a failure in one will result in a failure in the second
        #I'm not sure if this is the best way, but it's the one users will use
        #The problem is that it's called in reverse order by unittest so you have to
        #remember that.
        # -mnorman

        print "getMaxEvents"

        testMask = Mask()
        maxEvents  = 100
        skipEvents = 0

        tempMax = testMask.getMaxEvents()

        self.assertEqual(tempMax, None)

        testMask.setMaxAndSkipEvents(maxEvents, skipEvents)

        tempMax = testMask.getMaxEvents()

        self.assertEqual(tempMax, maxEvents + skipEvents)



if __name__ == '__main__':
    unittest.main()
