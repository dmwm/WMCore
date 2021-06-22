#!/usr/bin/env python
"""
    WorkQueueElement unit tests
"""

from builtins import range
import unittest
import itertools

from Utils.PythonVersion import PY3
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement, possibleSites

RequestNames = ['test', 'test2', '', 'abcdefg']
TaskNames = [None, '', 'task1', 'task2', 'abcdfg']
Inputs = [{}, {'/dataset/RAW': ['sitea']},
          {'/dataset/RAW': ['sitea'], '/dataset/RECO': ['sitea', 'siteb']}]
Masks = [None, {'FirstEvent': 1}, {'FirstEvent': 1, 'LastEvent': 10},
         {'FirstEvent': 5, 'LastLumi': 10}]
Dbses = [None, 'https://example.com/dbs', 'http://example.com/another_dbs']
Progress = Priority = [x for x in range(0, 100, 10)]
Teams = [None, '', 'bob', 'A-Team']
ParentQueueUrl = [None, '', 'https://something']
ParentQueueId = [None, '1', 2]
Acdcs = [{}, {'Something': 'Somewhat'}]


class WorkQueueElementTest(unittest.TestCase):
    def setUp(self):
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def testId(self):
        """Id calculated correctly"""
        ele = WorkQueueElement(RequestName='test')
        self.assertEqual(ele.id, '8b2e394154f6464f4b2aaf086a45f8c2')

    def testIdUnique(self):
        """Modifying a relevant parameter varies the id"""
        ids = {}
        # Vary parameters that affect the work or input data,
        # verify each id is unique
        for params in itertools.product(RequestNames, TaskNames, Inputs,
                                        Masks, Dbses, Acdcs):
            ele = WorkQueueElement(RequestName=params[0], TaskName=params[1],
                                   Inputs=params[2], Mask=params[3],
                                   Dbs=params[4], ACDC=params[5]
                                  )
            self.assertFalse(ele.id in ids)
            ids[ele.id] = None

    def testIdIgnoresIrrelevant(self):
        """Id calculation ignores irrelavant variables"""
        ele = WorkQueueElement(RequestName='testIdIgnoresIrrelevant')
        this_id = ele.id
        for params2 in itertools.product(Progress, Priority, Teams,
                                         ParentQueueUrl, ParentQueueId):
            ele = WorkQueueElement(RequestName='testIdIgnoresIrrelevant',
                                   PercentSuccess=params2[0],
                                   Priority=params2[1], TeamName=params2[2],
                                   ParentQueueUrl=params2[3], ParentQueueId=params2[4],
                                  )
            # id not changed by changing irrelvant parameters
            self.assertEqual(ele.id, this_id)

    def testIdImmutable(self):
        """Id fixed once calculated"""
        ele = WorkQueueElement(RequestName='testIdImmutable')
        before_id = ele.id
        ele['RequestName'] = 'somethingElse'
        self.assertEqual(before_id, ele.id)

    def testSetId(self):
        """Can override id generation"""
        ele = WorkQueueElement(RequestName='testIdImmutable')
        before_id = ele.id
        ele.id = 'something_new'
        self.assertEqual('something_new', ele.id)
        self.assertNotEqual(before_id, ele.id)

    def testPassesSiteRestriction(self):
        """
        Workqueue element site restriction check (same as workRestrictions)
        """
        # test element ala MonteCarlo
        ele = WorkQueueElement(SiteWhitelist=["T1_IT_CNAF", "T2_DE_DESY"], SiteBlacklist=["T1_US_FNAL"])
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))

        # test element with input dataset
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": []}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertFalse(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertFalse(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))

        # test element with input and parent dataset
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": []}
        ele['ParentFlag'] = True
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": []}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertFalse(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertFalse(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_DE_DESY"]}
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": ["T1_IT_CNAF", "T2_CH_CERN", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))

        # test element with input, parent and pileup dataset
        ele['PileupData'] = {"/MY/DATASET/NAME": []}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertFalse(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertFalse(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['PileupData'] = {"/MY/DATASET/NAME": ["T2_US_Nebraska", "T1_IT_CNAF"]}
        self.assertFalse(ele.passesSiteRestriction("T1_IT_CNAF"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T1_IT_CNAF", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertFalse(ele.passesSiteRestriction("T2_DE_DESY"))

    def testPassesSiteRestrictionLocationFlags(self):
        """
        Workqueue element site restriction check (same as workRestrictions)
        """
        # test element ala MonteCarlo
        ele = WorkQueueElement(SiteWhitelist=["T1_IT_CNAF", "T2_DE_DESY"], SiteBlacklist=["T1_US_FNAL"])
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))

        # test element with input dataset
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": []}
        ele['NoInputUpdate'] = True
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))

        # test element with input and parent dataset
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": []}
        ele['ParentFlag'] = True
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": []}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_DE_DESY"]}
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": ["T1_IT_CNAF", "T2_CH_CERN", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))

        # test element with input, parent and pileup dataset
        ele['PileupData'] = {"/MY/DATASET/NAME": []}
        ele['NoPileupUpdate'] = True
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))
        ele['PileupData'] = {"/MY/DATASET/NAME": ["T2_US_Nebraska", "T1_IT_CNAF"]}
        self.assertFalse(ele.passesSiteRestriction("T2_US_Nebraska"))
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T1_IT_CNAF", "T2_DE_DESY"]}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))
        # only the pileup flag enabled now
        ele['NoInputUpdate'] = False
        ele['PileupData'] = {"/MY/DATASET/NAME": []}
        self.assertFalse(ele.passesSiteRestriction("T1_US_FNAL"))
        self.assertFalse(ele.passesSiteRestriction("T2_CH_CERN"))
        self.assertTrue(ele.passesSiteRestriction("T1_IT_CNAF"))
        self.assertTrue(ele.passesSiteRestriction("T2_DE_DESY"))

    def testPossibleSites(self):
        """
        Workqueue element data location check (same as workRestrictions)
        """
        # test element ala MonteCarlo
        ele = WorkQueueElement(SiteWhitelist=["T1_IT_CNAF", "T2_DE_DESY"])
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])
        # test element with InputDataset but no location
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": []}
        self.assertEqual(possibleSites(ele), [])
        # test element with InputDataset and no match location
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_CH_CERN"]}
        self.assertEqual(possibleSites(ele), [])
        # test element with InputDataset and valid location
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_CH_CERN", "T2_DE_DESY"]}
        self.assertEqual(possibleSites(ele), ["T2_DE_DESY"])

        # test element with InputDataset and ParentData with no location
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_CH_CERN", "T2_DE_DESY"]}
        ele['ParentFlag'] = True
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": []}
        self.assertEqual(possibleSites(ele), [])
        # test element with InputDataset and ParentData with no match location
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": ["T1_IT_CNAF"]}
        self.assertEqual(possibleSites(ele), [])
        # test element with InputDataset and ParentData with valid location
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": ["T1_US_FNAL", "T2_DE_DESY"]}
        self.assertEqual(possibleSites(ele), ["T2_DE_DESY"])

        # test element with InputDataset, PileupData and ParentData with no location
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_CH_CERN", "T2_DE_DESY"]}
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": ["T2_DE_DESY"]}
        ele['PileupData'] = {"/MY/DATASET/NAME": []}
        self.assertEqual(possibleSites(ele), [])
        # test element with InputDataset, PileupData and ParentData with no match location
        ele['PileupData'] = {"/MY/DATASET/NAME": ["T1_IT_CNAF", "T2_CH_CERN"]}
        self.assertEqual(possibleSites(ele), [])
        # test element with InputDataset, PileupData and ParentData with valid location
        ele['PileupData'] = {"/MY/DATASET/NAME": ["T1_IT_CNAF", "T2_DE_DESY"]}
        self.assertEqual(possibleSites(ele), ["T2_DE_DESY"])

    def testPossibleSitesLocationFlags(self):
        """
        Workqueue element data location check, using the input and PU data location flags
        """
        ele = WorkQueueElement(SiteWhitelist=["T1_IT_CNAF", "T2_DE_DESY"])

        # test element with InputDataset and no location, but input flag on
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": []}
        ele['NoInputUpdate'] = True
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])
        # test element with InputDataset and one match, but input flag on
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_IT_CNAF", "T2_CH_CERN"]}
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])
        # test element with InputDataset and one match, but pu flag on
        ele['NoInputUpdate'] = False
        ele['NoPileupUpdate'] = True
        self.assertEqual(possibleSites(ele), ["T1_IT_CNAF"])
        # test element with InputDataset and one match, but both flags on
        ele['NoInputUpdate'] = True
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])

        # test element with InputDataset and ParentData and no location, but both flags on
        ele['ParentFlag'] = True
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": []}
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])
        # test element with InputDataset and ParentData and no location, but input flag on
        ele['NoPileupUpdate'] = False
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])
        # test element with InputDataset and ParentData and no location, but pileup flag on
        ele['NoInputUpdate'] = False
        ele['NoPileupUpdate'] = True
        self.assertEqual(possibleSites(ele), [])

        # test element with InputDataset, PileupData and ParentData with no location, but pileup flag on
        ele['Inputs'] = {"/MY/BLOCK/NAME#73e99a52": ["T1_US_FNAL", "T2_CH_CERN", "T2_DE_DESY"]}
        ele['ParentData'] = {"/MY/BLOCK2/NAME#002590494c06": ["T2_DE_DESY"]}
        ele['PileupData'] = {"/MY/DATASET/NAME": []}
        self.assertEqual(possibleSites(ele), ["T2_DE_DESY"])
        # test element with InputDataset, PileupData and ParentData with no location, but both flags on
        ele['NoInputUpdate'] = True
        self.assertItemsEqual(possibleSites(ele), ["T1_IT_CNAF", "T2_DE_DESY"])
        # test element with InputDataset, PileupData and ParentData with no location, but input flag on
        ele['NoPileupUpdate'] = False
        self.assertEqual(possibleSites(ele), [])


if __name__ == '__main__':
    unittest.main()
