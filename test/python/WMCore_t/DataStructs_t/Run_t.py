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

        run4 = Run(1000000, 1, 2, 3, 4, 5)
        self.assertEqual(run4.run, 1000000)
        self.assertEqual(len(run4), 5)
        self.assertEqual(run4[0], 1)
        self.assertEqual(run4[4], 5)

        # using new DBS lumi/event format
        run5 = Run(555, [1, 2, 3])
        self.assertEqual(run5.run, 555)
        self.assertEqual(len(run5), 3)
        self.assertDictEqual(run5.eventsPerLumi, {1: None, 2: None, 3: None})

        run6 = Run(666, [(1, 11), (2, 22), (3, 33)])
        self.assertEqual(run6.run, 666)
        self.assertEqual(len(run6), 3)
        self.assertDictEqual(run6.eventsPerLumi, {1: 11, 2: 22, 3: 33})

        run7 = Run(666, [(1, 11), (2, 22), (3, 33), (1, 4), (3, 2)])
        self.assertEqual(run7.run, 666)
        self.assertEqual(len(run7), 3)
        self.assertDictEqual(run7.eventsPerLumi, {1: 15, 2: 22, 3: 35})

        run8 = Run(666, [(1, None), (2, None), (3, None)])
        self.assertEqual(run8.run, 666)
        self.assertEqual(len(run8), 3)
        self.assertDictEqual(run8.eventsPerLumi, {1: None, 2: None, 3: None})

        run10 = Run(555, *[1, 2, 3])
        self.assertEqual(run10.run, 555)
        self.assertEqual(len(run10), 3)
        self.assertDictEqual(run10.eventsPerLumi, {1: None, 2: None, 3: None})

        run11 = Run(666, *[(1, None), (2, None), (3, None)])
        self.assertEqual(run11.run, 666)
        self.assertDictEqual(run11.eventsPerLumi, {1: None, 2: None, 3: None})
        self.assertEqual(len(run11), 3)

        run13 = Run(666, *[(1, 11), (2, 22), (3, 33), (1, 4), (3, 2)])
        self.assertEqual(run13.run, 666)
        self.assertEqual(len(run13), 3)
        self.assertDictEqual(run13.eventsPerLumi, {1: 15, 2: 22, 3: 35})

    def test2(self):
        """
        test equality & addition operators

        """
        run1 = Run(1, 1, 2, 3, 4, 5)
        run1copy = Run(1, 1, 2, 3, 4, 5)
        run1difflumi = Run(1, 6, 7, 8, 9, 10)
        run1overlap = Run(1, 3, 4, 5, 6, 7)
        run2 = Run(2, 1, 2, 3, 4, 5)

        self.assertTrue(run1 == run1copy)
        self.assertFalse(run1 == run1difflumi)
        self.assertFalse(run1 == run1overlap)
        self.assertFalse(run1 == run2)
        self.assertTrue(run1 != run2)

        run1 += run1difflumi
        self.assertEqual(len(run1), 10)
        for l in run1difflumi:
            self.assertTrue(l in run1)

    def testC(self):
        """
        comparision and sorting

        """
        def cmp_runs(run_x, run_y):
            return [
                run_x < run_y, run_x <= run_y,
                run_x == run_y, run_x != run_y,
                run_x > run_y, run_x >= run_y,]

        run1 = Run(1, 1, 2, 3)
        run2 = Run(2, 4, 5, 6)
        run3 = Run(3, 7, 8, 9)

        runlist = list(sorted([run2, run3, run1]))
        self.assertListEqual(runlist, [run1, run2, run3])
        self.assertListEqual(cmp_runs(run1, run2),
                             [True, True, False, True, False, False])

        run4 = Run(4, 1, 2, 3)
        run5 = Run(4, 4, 5, 6)
        run6 = Run(4, 7, 8, 9)

        runlist = list(sorted([run5, run6, run4]))
        self.assertListEqual(runlist, [run4, run5, run6])

        runlist = list(sorted([run5, run1, run3, run4, run6, run2]))
        self.assertListEqual(runlist, [run1, run2, run3, run4, run5, run6])

        run7  = Run(9, 1, 2)
        run8  = Run(9, 1, 2, 3)
        run9  = Run(9, 1, 2, 3)
        run10 = Run(9, 1, 2, 3, 4)
        run11 = Run(9, 1, 2, 4)
        run12 = Run(9, 1, 4)
        run13 = Run(9, 2)
        run14 = Run(9, 3, 4, 5)
        run15 = Run(9, 8, 9, 10)

        runlist = list(sorted([run10, run15, run8,  run11, run13,
                               run7,  run9,  run14, run12]))
        self.assertListEqual(runlist,
                            [run7, run8, run9, run10, run11,
                             run12, run13, run14, run15])

        self.assertListEqual(cmp_runs(run14, run15),
                             [True, True, False, True, False, False])
        self.assertListEqual(cmp_runs(run8, run8),
                             [False, True, True, False, False, True])
        self.assertListEqual(cmp_runs(run8, run9),
                             [False, True, True, False, False, True])

        run20 = Run(666, [(1, 11), (2, 22), (3, 33)])
        run21 = Run(666, [(1, 11), (2, 22), (3, 33)])
        run22 = Run(666, [(1, 11), (2, 22), (3, 32)])
        run23 = Run(666, [(1, 11), (2, 22), (3, 34)])
        run24 = Run(666, [(1, 11), (2, 21), (3, 33)])
        run25 = Run(666, [(1, 11), (2, 21), (3, 34)])
        run26 = Run(666, [(1, 11), (2, 23), (3, 32)])

        self.assertListEqual(cmp_runs(run20, run21),
                             [False, True, True, False, False, True])
        self.assertListEqual(cmp_runs(run20, run22),
                             [False, False, False, True, True, True])
        self.assertListEqual(cmp_runs(run20, run23),
                             [True, True, False, True, False, False])
        self.assertListEqual(cmp_runs(run20, run24),
                             [False, False, False, True, True, True])
        self.assertListEqual(cmp_runs(run20, run25),
                             [False, False, False, True, True, True])
        self.assertListEqual(cmp_runs(run20, run26),
                             [True, True, False, True, False, False])

    def testD(self):
        """
        test hashing for sets

        """
        run1 = Run(1, 1, 2, 3)
        run2 = Run(2, 4, 5, 6)
        run3 = Run(3, 7, 8, 9)
        run4 = Run(4, 1, 2, 3)
        run5 = Run(4, 4, 5, 6)
        run6 = Run(4, 7, 8, 9)
        run7 = Run(9, 1, 2, 3)
        run8 = Run(9, 1, 2, 3)
        run9 = Run(9, 2)
        run10 = Run(9, 3, 4, 5)
        run11 = Run(9, 8, 9, 10)

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
        self.assertEqual(run7, run8)
        self.assertNotEqual(run7, run9)
        self.assertNotEqual(run7, run10)

    def testNewHash(self):
        run1 = Run(1, 1, 2, 3)
        self.assertEqual(hash(run1), 79038151938585768)
        run666 = Run(666, [(1, 111), (3, 33), (2, 22)])
        self.assertEqual(hash(run666), 21721583371131476)


if __name__ == '__main__':
    unittest.main()
