#!/usr/bin/env python
"""
_DBSUtils_t_

Unit test for the DBSUtils module
"""

import time
import unittest
import mock

from nose.plugins.attrib import attr

import WMCore.Services.DBS.DBSUtils
from WMCore.Services.DBS.DBSUtils import urlParams, DBSErrors
from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMQuality.Emulators.DBS.DBSUtils import MockDBSErrors


class DBSUtilsTest(unittest.TestCase):
    """
    DBSUtilsTest represent unit test class
    """

    def testUrlParams(self):
        """
        urlParams should return dictionary of URL parameters
        """
        url = 'http://a.b.com?d=1&f=bla'
        results = urlParams(url)
        self.assertCountEqual(results, {'d': '1', 'f': 'bla'})
        self.assertTrue(results.get('d'), 1)
        self.assertTrue(results.get('f'), 'bla')

        url = 'http://a.b.com?d=1&f=bla&d=2'
        results = urlParams(url)
        self.assertCountEqual(results, {'d': ['1', '2'], 'f': 'bla'})

    @attr("integration")
    def testGetParallelListDatasetFileDetails(self):
        """
        test parallel execution of listDatasetFileDetails DBS API
        We use small dataset with the following characteristics:

                dasgoclient -query="dataset=/VBF1Parked/HIRun2013A-v1/RAW summary" | jq
                [
                  {
                        "file_size": 6053097,
                        "nblocks": 7,
                        "nevents": 0,
                        "nfiles": 7,
                        "nlumis": 428,
                        "num_block": 7,
                        "num_event": 0,
                        "num_file": 7,
                        "num_lumi": 428
                  }
                ]

                The parallel call should perform better then sequential while results
        should remain the same.

        """
        url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        reader1 = DBS3Reader(url, logger=None, parallel=False, aggregate=True)
        reader2 = DBS3Reader(url, logger=None, parallel=True, aggregate=True)
        dataset = '/VBF1Parked/HIRun2013A-v1/RAW'
        time0 = time.time()
        res1 = reader1.listDatasetFileDetails(dataset)
        time1 = time.time() - time0
        self.assertTrue(time1 > 0)  # to avoid pyling complaining about not used varaiable
        time0 = time.time()
        res2 = reader2.listDatasetFileDetails(dataset)
        time2 = time.time() - time0
        self.assertTrue(time2 > 0)  # to avoid pyling complaining about not used varaiable
        self.assertTrue(res1 == res2)

    @attr("integration")
    def testGetParallelListFileBlockLocation(self):
        """
        test parallel execution of listFileBlockLocation DBS API
        We use small dataset with the following data:

        dasgoclient -query="block dataset=/VBF1Parked/HIRun2013A-v1/RAW"

        The parallel call should perform better then sequential while results
        should remain the same.

        """
        url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        reader1 = DBS3Reader(url, logger=None, parallel=False, aggregate=True)
        reader2 = DBS3Reader(url, logger=None, parallel=True, aggregate=True)
        blocks = [
            '/VBF1Parked/HIRun2013A-v1/RAW#52da10be-5c87-11e2-912f-842b2b4671d8',
            '/VBF1Parked/HIRun2013A-v1/RAW#6861d50a-5c7f-11e2-912f-842b2b4671d8',
            '/VBF1Parked/HIRun2013A-v1/RAW#6dd88910-5c80-11e2-912f-842b2b4671d8',
            '/VBF1Parked/HIRun2013A-v1/RAW#6e258f12-5c80-11e2-912f-842b2b4671d8',
            '/VBF1Parked/HIRun2013A-v1/RAW#fc64292c-5c81-11e2-912f-842b2b4671d8',
            '/VBF1Parked/HIRun2013A-v1/RAW#fc87c8dc-5c81-11e2-912f-842b2b4671d8',
            '/VBF1Parked/HIRun2013A-v1/RAW#fcd9876c-5c81-11e2-912f-842b2b4671d8'
            ]
        time0 = time.time()
        res1 = reader1.listFileBlockLocation(blocks)
        time1 = time.time() - time0
        self.assertTrue(time1 > 0)  # to avoid pyling complaining about not used varaiable
        time0 = time.time()
        res2 = reader2.listFileBlockLocation(blocks)
        time2 = time.time() - time0
        self.assertTrue(time2 > 0)  # to avoid pyling complaining about not used varaiable
        self.assertTrue(res1 == res2)

    @attr("integration")
    def testGetParallelGetParentFilesGivenParentDataset(self):
        """
        test parallel execution of getParentFilesGivenParentDataset DBS API
        We use small the following data:

        # find lfn for some dataset
        dasgoclient -query="file dataset=/VBF1Parked/Run2012D-22Jan2013-v1/AOD"
        ...
        /store/data/Run2012D/VBF1Parked/AOD/22Jan2013-v1/120000/F64DFA15-15A8-E211-9277-80000048FE80.root

        # find parent dataset
        dasgoclient -query="parent dataset=/VBF1Parked/Run2012D-22Jan2013-v1/AOD"
        /VBF1Parked/Run2012D-v1/RAW

        The parallel call should perform better then sequential while results
        should remain the same.

        """
        url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        reader1 = DBS3Reader(url, logger=None, parallel=False, aggregate=True)
        reader2 = DBS3Reader(url, logger=None, parallel=True, aggregate=True)
        parentDataset = '/VBF1Parked/Run2012D-v1/RAW'
        childLFN = '/store/data/Run2012D/VBF1Parked/AOD/22Jan2013-v1/120000/F64DFA15-15A8-E211-9277-80000048FE80.root'
        time0 = time.time()
        res1 = reader1.getParentFilesGivenParentDataset(parentDataset, childLFN)
        time1 = time.time() - time0
        self.assertTrue(time1 > 0)  # to avoid pyling complaining about not used varaiable
        time0 = time.time()
        res2 = reader2.getParentFilesGivenParentDataset(parentDataset, childLFN)
        time2 = time.time() - time0
        self.assertTrue(time2 > 0)  # to avoid pyling complaining about not used varaiable
        self.assertTrue(res1 == res2)

    @attr("integration")
    def testDBSErrors(self):
        """
        integration test for DBSErrors function
        """
        url = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
        dbsErrors = DBSErrors(url)
        self.assertTrue(100 in dbsErrors)
        self.assertTrue(101 in dbsErrors)
        self.assertTrue(300 in dbsErrors)
        self.assertTrue(dbsErrors[100] == 'generic DBS error')

    def testMockDBSErrors(self):
        """
        mock unit test for DBSErrors function
        """
        with mock.patch('WMCore.Services.DBS.DBSUtils.DBSErrors', new=MockDBSErrors):
            dbsErrors = WMCore.Services.DBS.DBSUtils.DBSErrors("")  # mocked DBSErrors function
            self.assertTrue(100 in dbsErrors)
            self.assertTrue(101 in dbsErrors)
            self.assertTrue(300 in dbsErrors)
            self.assertTrue(dbsErrors[100] == 'generic DBS error')


if __name__ == '__main__':
    unittest.main()
