#!/usr/bin/env python

import unittest

from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import SubscriptionList, PhEDExSubscription

class SubscriptionListTest(unittest.TestCase):
    """
    _SubscriptionListTest_

    Test class for the subscription list data structure
    """

    def setUp(self):
        """
        _setUp_

        Nothing to setup
        """
        pass

    def tearDown(self):
        """
        _tearDown_

        Nothing to tear down
        """
        pass

    def testSubscriptionList(self):
        """
        _SubscriptionList_

        Check that we can organize and agreggate correctly a bunch of different subscriptions
        in standard scenarios
        """
        subList = SubscriptionList()
        # Two GEN datasets subscribed to many sites non-custodially and custodially to one
        subs = []
        genDatasetA = "/DeadlyNeurotoxinOnTestSubjectSim/Run1970-Test-v2/GEN"
        subs.append({"datasetPathList" : genDatasetA, "nodeList" : "T1_US_FNAL",
                     "group" : "dataops", "custodial" : "y", "move" : "y"})
        subs.append({"datasetPathList" : genDatasetA, "nodeList" : "T2_IT_Bari",
                     "group" : "dataops", "request_only" : "n"})
        subs.append({"datasetPathList" : genDatasetA, "nodeList" : "T2_CH_CERN",
                     "group" : "dataops", "request_only" : "n"})
        subs.append({"datasetPathList" : genDatasetA, "nodeList" : "T2_US_Wisconsin",
                     "group" : "dataops"})
        genDatasetB = "/NotEnoughEnergyToLieIn1.1V/Run1970-Potato-v2/GEN"
        subs.append({"datasetPathList" : genDatasetB, "nodeList" : "T1_IT_CNAF",
                     "group" : "dataops", "custodial" : "y", "move" : "y"})
        subs.append({"datasetPathList" : genDatasetB, "nodeList" : "T2_IT_Bari",
                     "group" : "dataops", "request_only" : "n"})
        subs.append({"datasetPathList" : genDatasetB, "nodeList" : "T2_CH_CERN",
                     "group" : "dataops", "request_only" : "n"})
        subs.append({"datasetPathList" : genDatasetB, "nodeList" : "T2_US_Wisconsin",
                     "group" : "dataops"})
        # RECO,DQM,AOD datasets subscribed custodially to 2 sites
        recoDatasetA = '/TestWeightedCubes/Run1970-Test-v2/%s'
        recoDatasetB = '/RepulsiveGel/Run1970-Test-v2/%s'
        subs.append({"datasetPathList" : recoDatasetA % 'AOD' , "nodeList" : "T1_US_FNAL",
                     "group" : "dataops", "custodial" : "y", "move" : "y"})
        subs.append({"datasetPathList" : recoDatasetA % 'DQM' , "nodeList" : "T1_US_FNAL",
                     "group" : "dataops", "custodial" : "y", "move" : "y"})
        subs.append({"datasetPathList" : recoDatasetB % 'AOD' , "nodeList" : "T1_DE_KIT",
                     "group" : "dataops", "custodial" : "y", "move" : "y"})
        subs.append({"datasetPathList" : recoDatasetB % 'DQM' , "nodeList" : "T1_DE_KIT",
                     "group" : "dataops", "custodial" : "y", "move" : "y"})

        for sub in subs:
            phedexSub = PhEDExSubscription(**sub)
            subList.addSubscription(phedexSub)

        # One subscription per node
        self.assertEqual(len(subList.getSubscriptionList()), 6)
        goldenDatasetLists = [set([genDatasetA, genDatasetB]),
                              set([genDatasetA, recoDatasetA % 'AOD',
                                   recoDatasetA % 'DQM']), set([genDatasetB]),
                              set([recoDatasetB % 'AOD', recoDatasetB % 'DQM'])]
        for sub in subList.getSubscriptionList():
            self.assertTrue(set(sub.getDatasetPaths()) in goldenDatasetLists)

        subList.compact()

        # Compact should have reduced 2 of them to 1
        goldenNodeLists = [set(["T1_US_FNAL"]), set(["T2_IT_Bari", "T2_CH_CERN"]),
                           set(["T1_IT_CNAF"]), set(["T1_DE_KIT"]), set(["T2_US_Wisconsin"])]
        self.assertEqual(len(subList.getSubscriptionList()), 5)
        for sub in subList.getSubscriptionList():
            self.assertTrue(set(sub.getNodes()) in goldenNodeLists)
        return

if __name__ == '__main__':
    unittest.main()
