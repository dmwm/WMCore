#!/usr/bin/env python
"""
    WorkQueue.Policy.End tests
"""




import unittest
from functools import partial
from WMCore.WorkQueue.Policy.End.EndPolicyInterface import EndPolicyInterface
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement as WQE


class EndPolicyInterfaceTestCase(unittest.TestCase):

    def setUp(self):
        """Create workflow stuff"""
        self.policy = partial(EndPolicyInterface)

        # ones i made earlier
        self.parent = WQE(); self.parent.id = 1
        self.available = WQE(Status = 'Available', ParentQueueId = 1)
        self.acquired = WQE(Status = 'Acquired', ParentQueueId = 1)
        self.negotiating = WQE(Status = 'Negotiating', ParentQueueId = 1)
        self.done = WQE(Status = 'Done', PercentComplete = 100, PercentSuccess = 100, ParentQueueId = 1)
        self.failed = WQE(Status = 'Failed', PercentComplete = 100, PercentSuccess = 0, ParentQueueId = 1)

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
            results = self.policy()(items, [self.parent])
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['Status'], 'Acquired')

    def testEndConditions(self):
        """Correct status when all elements in an end state"""
        for result in (('Done', self.done, self.done),
                       ('Failed', self.failed, self.done)):
            self.assertEqual(result[0], self.policy()(result[1:], [self.parent])[0]['Status'])

    def testNegotiating(self):
        """Workflow only partially split"""
        # one element split and available in workqueue, other not split yet
        parent2 = WQE(Status = 'Negotiating')
        parent2.id = 2
        parents = [self.parent, parent2]
        results = self.policy()([self.available], parents)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['Status'], 'Acquired')
        self.assertEqual(results[1]['Status'], 'Negotiating')

if __name__ == '__main__':
    unittest.main()
