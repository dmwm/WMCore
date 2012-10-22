#!/usr/bin/env python

import unittest

from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import SubscriptionList, PhEDExSubscription

class SubscriptionListTest(unittest.TestCase):
    """
    _subscriptionListTest_

    I don't know what this does.
    """

    def setUp(self):
        """
        _setUp_

        Nothing
        """
        return

    def tearDown(self):
        """
        _tearDown_

        Nothing
        """
        return


    def test_SubscriptionList(self):
        """
        _SubscriptionList_

        Whatever
        """

        policy = SubscriptionList()
        # what will you do with run ID.
        row = [6, "/Cosmics/Test-CRAFTv8/RAW",1, "T2_CH_CAF" , 'high', 'y']
        results = []
        for i in range(6):
            results.append(row)

        results.append([7, "/Cosmics/Test-CRAFTv8/ALCARECO", 2, "FNAL" , 'normal', 'y'])
        results.append([8, "/Cosmics/Test-CRAFTv8/RECO", 1, "T2_CH_CAF" , 'high', 'y'])
        results.append([8, "/Cosmics/Test-CRAFTv8/RECO", 2, "FNAL" , 'high', 'y'])

        print policy.getSubscriptionList()

        for row in results:
            # make a tuple (dataset_path_id, dataset_path_name)
            # make a tuple (node_id, node_name)
            subscription = PhEDExSubscription((row[0], row[1]), (row[2], row[3]),
                                              row[4], row[5])
            policy.addSubscription(subscription)

        i = 0
        for sub in policy.getSubscriptionList():
            i += 1
            print "Subscription %s" % i
            print sub.getDatasetPaths()
            print sub.getNodes()

        return

if __name__ == '__main__':
    unittest.main()
