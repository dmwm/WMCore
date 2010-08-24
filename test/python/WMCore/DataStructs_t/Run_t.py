#!/usr/bin/env python
"""
_Run_

Unittest for the WMCore.DataStructs.Run class

"""


import unittest
from WMCore.DataStructs.Run import Run


class RunTest(unittest.TestCase):
    """
    _RunTest_

    """

    def testA(self):
        """instantiation"""

        run1 = Run()
        self.assertEqual(run1.run, None)
        self.assertEqual(len(run1), 0)


        run2 = Run(1000000)
        self.assertEqual(run2.run, 1000000)
        self.assertEqual(len(run2), 0)

        run3 = Run(1000000, 1)
        self.assertEqual(run3.run, 1000000)
        self.assertEqual(len(run3), 1)
        self.assertEqual(run3[0], 1)


        run4 = Run(1000000, 1,2,3,4,5)
        self.assertEqual(run4.run, 1000000)
        self.assertEqual(len(run4), 5)
        self.assertEqual(run4[0], 1)
        self.assertEqual(run4[4], 5)


    def test2(self):
        """
        test equality & addition operators

        """
        run1 = Run(1, 1,2,3,4,5)
        run1copy = Run(1, 1, 2,3,4,5)
        run1difflumi = Run(1, 6,7,8,9,10)
        run1overlap  = Run(1, 3,4,5,6,7)
        run2 = Run(2, 1,2,3,4,5)
        run3 = Run(2, 6,7,8,9,10)

        self.assertTrue( run1 == run1copy)
        self.assertFalse(run1 == run1difflumi)
        self.assertFalse(run1 == run1overlap)
        self.assertFalse(run1 == run2)
        self.assertTrue(run1 != run2)


        run1 += run1difflumi
        self.assertEqual(len(run1), 10)
        for l in run1difflumi:
            self.failUnless(l in run1)



    def testC(self):
        """
        comparision and sorting

        """
        run1 = Run(1, 1,2,3)
        run2 = Run(2, 4,5,6)
        run3 = Run(3, 7,8,9)

        runlist = [run2, run3, run1]
        runlist.sort()
        self.assertEqual(runlist[0], run1)
        self.assertEqual(runlist[1], run2)
        self.assertEqual(runlist[2], run3)


        run4 = Run(4, 1,2,3)
        run5 = Run(4, 4,5,6)
        run6 = Run(4, 7,8,9)

        runlist = [run5, run6, run4]
        runlist.sort()
        self.assertEqual(runlist[0], run4)
        self.assertEqual(runlist[1], run5)
        self.assertEqual(runlist[2], run6)

        runlist = [run5, run1, run3, run4, run6, run2]

        runlist.sort()
        self.assertEqual(runlist[0], run1)
        self.assertEqual(runlist[1], run2)
        self.assertEqual(runlist[2], run3)
        self.assertEqual(runlist[3], run4)
        self.assertEqual(runlist[4], run5)
        self.assertEqual(runlist[5], run6)


        run7 = Run(9, 1,2,3)
        run8 = Run(9, 1,2,3)
        run9 = Run(9, 2)
        run10 = Run(9, 3, 4, 5)
        run11 = Run(9, 8,9,10)


        runlist = [run10, run8, run11, run7, run9]
        runlist.sort()
        self.assertEqual(runlist[0], run7)
        self.assertEqual(runlist[1], run8)
        self.assertEqual(runlist[2], run9)
        self.assertEqual(runlist[3], run10)
        self.assertEqual(runlist[4], run11)


    def testD(self):
        """
        test hashing for sets

        """
        run1 = Run(1, 1,2,3)
        run2 = Run(2, 4,5,6)
        run3 = Run(3, 7,8,9)
        run4 = Run(4, 1,2,3)
        run5 = Run(4, 4,5,6)
        run6 = Run(4, 7,8,9)
        run7 = Run(9, 1,2,3)
        run8 = Run(9, 1,2,3)
        run9 = Run(9, 2)
        run10 = Run(9, 3, 4, 5)
        run11 = Run(9, 8,9,10)

        s = set()
        s.add(run1)
        s.add(run2)
        s.add(run3)
        s.add(run4)
        s.add(run5)
        s.add(run6)
        s.add(run7)
        s.add(run8)
        s.add(run9)
        s.add(run10)
        s.add(run11)





if __name__ == '__main__':
    unittest.main()
