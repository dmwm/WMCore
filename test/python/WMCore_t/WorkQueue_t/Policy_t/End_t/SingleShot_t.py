#!/usr/bin/env python
"""
    WorkQueue.Policy.End.SingleShot tests
"""




import unittest
import math
from WMCore.WorkQueue.Policy.End.SingleShot import SingleShot
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement as WQE


class SingleShotTestCase(unittest.TestCase):

    def setUp(self):
        """Create workflow stuff"""
        self.policy = SingleShot()
        self.strict_policy = SingleShot(SuccessThreshold = 1.0)

        # ones i made earlier
        self.available = WQE(Status = 'Available')
        self.acquired = WQE(Status = 'Acquired')
        self.negotiating = WQE(Status = 'Negotiating')
        self.done = WQE(Status = 'Done')
        self.failed = WQE(Status = 'Failed')

    def tearDown(self):
        pass

    def testOverallStatus(self):
        """Overall status correct for given inputs"""

        # check no final state till all elements in end state
        for items in ((self.available, self.available),
                      (self.acquired, self.available),
                      (self.available, self.acquired),
                      (self.available, self.failed),
                      (self.available, self.done),
                      (self.available, self.negotiating),
                      ):
            self.assertEqual(self.policy(*items)['Status'], 'Available')


    def testEndConditions(self):
        """Correct status when all elements in an end state"""
        for result in (('Done', self.done, self.done),
                       ('Failed', self.failed, self.done)):
            self.assertEqual(result[0], self.strict_policy(*result[1:])['Status'])


    def testSuccessThreshold(self):
        """Check threshold for success"""
        # range doesn't work with decimals?
        # Source: http://code.activestate.com/recipes/66472/
        def frange4(end, start = 0, inc = 0, precision = 1):
            """A range function that accepts float increments."""
            if not start:
                start = end + 0.0
                end = 0.0
            else: end += 0.0

            if not inc:
                inc = 1.0
            count = int(math.ceil((start - end) / inc))

            L = [None] * count

            L[0] = end
            for i in (xrange(1, count)):
                L[i] = L[i - 1] + inc
            return L

        # create dict with appropriate percentage of success/failures
        elements = {}
        for i in range(0, 100, 5):
            elements[i / 100.] = [self.done] * i + [self.failed] * (100 - i)

        # go through range, checking correct status for entire pre-seeded dict
        for threshold in frange4(0., 1., 0.05):
            policy = SingleShot(SuccessThreshold = threshold)
            for value, items in elements.items():
                if value >= threshold:
                    self.assertEqual(policy(*items)['Status'], 'Done')
                else:
                    self.assertEqual(policy(*items)['Status'], 'Failed')


if __name__ == '__main__':
    unittest.main()
