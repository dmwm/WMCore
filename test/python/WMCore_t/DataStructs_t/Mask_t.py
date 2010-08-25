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


    def testSetMaxAndSkipLumis(self):
        """
        test class for setMaxAndSkipLumis in Mask.py

        """

        print "testSetMaxAndSkipLumis"

        testMask  = Mask()
        maxLumis  = 10
        skipLumis = 2

        testMask.setMaxAndSkipLumis(maxLumis, skipLumis)

        self.assertEqual(testMask['FirstLumi'], skipLumis)
        self.assertEqual(testMask['LastLumi'],  maxLumis + skipLumis)

        return


    def testSetMaxAndSkipRuns(self):
        """
        test class for setMaxAndSkipRuns in Mask.py

        """

        print "testSetMaxAndSkipRuns"

        testMask  = Mask()
        maxRuns   = 1000
        skipRuns  = 200

        testMask.setMaxAndSkipRuns(maxRuns, skipRuns)

        self.assertEqual(testMask['FirstRun'], skipRuns)
        self.assertEqual(testMask['LastRun'],  maxRuns + skipRuns)

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


    def testGetMax(self):
        """
        test class for the getMax() routine added to Mask.py

        """

        print "testGetMax"

        testMask  = Mask()
        maxRuns   = 1000
        skipRuns  = 200

        testMask.setMaxAndSkipRuns(maxRuns, skipRuns)

        self.assertEqual(testMask.getMax('Lumi'), None)
        self.assertEqual(testMask.getMax('junk'), None)
        self.assertEqual(testMask.getMax('Run'),  1000)



if __name__ == '__main__':
    unittest.main()
