#!/usr/bin/env python
"""
    WorkQueueElement unit tests
"""

import unittest
import itertools

from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement

RequestNames = ['test', 'test2', '', 'abcdefg']
TaskNames = [None, '', 'task1', 'task2', 'abcdfg']
Inputs = [{}, {'/dataset/RAW' : ['sitea']},
          {'/dataset/RAW' : ['sitea'], '/dataset/RECO' : ['sitea', 'siteb']}]
Masks = [None, {'FirstEvent' : 1}, {'FirstEvent' : 1, 'LastEvent' : 10},
         {'FirstEvent' : 5, 'LastLumi' : 10}]
Dbses = [None, 'https://example.com/dbs', 'http://example.com/another_dbs']
Progress = Priority = [x for x in range(0, 100, 10)]
Teams = [None, '', 'bob', 'A-Team']
ParentQueueUrl = [None, '', 'https://something']
ParentQueueId = [None, '1', 2]
Acdcs = [{}, {'Something' : 'Somewhat'}]



class WorkQueueElementTest(unittest.TestCase):

    def testId(self):
        """Id calculated correctly"""
        ele = WorkQueueElement(RequestName = 'test')
        self.assertEqual(ele.id, '8b2e394154f6464f4b2aaf086a45f8c2')

    def testIdUnique(self):
        """Modifying a relevant parameter varies the id"""
        ids = {}
        # Vary parameters that affect the work or input data,
        # verify each id is unique
        for params in itertools.product(RequestNames, TaskNames, Inputs,
                                        Masks, Dbses, Acdcs):
            ele = WorkQueueElement(RequestName = params[0], TaskName = params[1],
                                   Inputs = params[2], Mask = params[3],
                                   Dbs = params[4], ACDC = params[5]
                                   )
            self.assertFalse(ele.id in ids)
            ids[ele.id] = None

    def testIdIgnoresIrrelevant(self):
        """Id calculation ignores irrelavant variables"""
        ele = WorkQueueElement(RequestName = 'testIdIgnoresIrrelevant')
        this_id = ele.id
        for params2 in itertools.product(Progress, Priority, Teams,
                                         ParentQueueUrl, ParentQueueId):
            ele = WorkQueueElement(RequestName = 'testIdIgnoresIrrelevant',
                                   PercentSuccess = params2[0],
                                   Priority = params2[1], TeamName = params2[2],
                                   ParentQueueUrl = params2[3], ParentQueueId = params2[4],
                                   )
            # id not changed by changing irrelvant parameters
            self.assertEqual(ele.id, this_id)

    def testIdImmutable(self):
        """Id fixed once calculated"""
        ele = WorkQueueElement(RequestName = 'testIdImmutable')
        before_id = ele.id
        ele['RequestName'] = 'somethingElse'
        self.assertEqual(before_id, ele.id)

    def testSetId(self):
        """Can override id generation"""
        ele = WorkQueueElement(RequestName = 'testIdImmutable')
        before_id = ele.id
        ele.id = 'something_new'
        self.assertEqual('something_new', ele.id)
        self.assertNotEqual(before_id, ele.id)

if __name__ == '__main__':
    unittest.main()
