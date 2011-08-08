#!/usr/bin/env python
"""
    WorkQueue.Policy.End.SingleShot tests
"""




import unittest
import math
from functools import partial
from WMCore.WorkQueue.Policy.End.SingleShot import SingleShot
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement as WQE


class SingleShotTestCase(unittest.TestCase):

    def setUp(self):
        """Create workflow stuff"""
        self.policy = partial(SingleShot)

        # ones i made earlier
        self.parent = WQE(); self.parent.id = 1
        self.available = WQE(Status = 'Available', ParentQueueId = 1)
        self.acquired = WQE(Status = 'Acquired', ParentQueueId = 1)
        self.negotiating = WQE(Status = 'Negotiating', ParentQueueId = 1)
        self.done = WQE(Status = 'Done', PercentComplete = 100, PercentSuccess = 100, ParentQueueId = 1)
        self.failed = WQE(Status = 'Failed', PercentComplete = 100, PercentSuccess = 0, ParentQueueId = 1)

    def tearDown(self):
        pass


    def testSuccessThreshold(self):
        """Check threshold for success"""
        # no custom functionality left - just calls base class
        pass


if __name__ == '__main__':
    unittest.main()
