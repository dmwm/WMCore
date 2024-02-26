#!/usr/bin/env python
"""
_DBSReader_t_

Unit test for the DBSConcurrency module
"""

import os
import json
import time
import logging
import unittest

# WMCore modules
from WMCore.Services.DBS.DBSConcurrency import getBlockInfo4PU
from WMCore.Services.pycurl_manager import getdata as multi_getdata
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


class DBSConcurrencyTest(EmulatedUnitTestCase):
    """
    DBSConcurrencyTest class defines unit tests for DBS concurrent codebase
    """

    def setUp(self):
        """
        Initialization function
        """

        self.dbs = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'
        self.ckey = os.getenv('X509_USER_KEY')
        self.cert = os.getenv('X509_USER_CERT')
        logging.basicConfig()
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

    def testGetBlockInfoList(self):
        """
        Unit test for getBlockInfo4PU function
        """
        time0 = time.time()
        dataset = '/ZMM_13TeV_TuneCP5-pythia8/RunIIAutumn18DR-SNB_102X_upgrade2018_realistic_v17-v2/AODSIM'
        url = f"{self.dbs}/blocks?dataset={dataset}"
        self.logger.info(url)
        results = multi_getdata([url], self.ckey, self.cert)
        blocks = []
        for row in results:
            data = json.loads(row['data'])
            blocks = [r['block_name'] for r in data]
        elapsedTime = time.time() - time0
        self.logger.debug("for %s get %d in %s seconds", dataset, len(blocks), elapsedTime)
        self.assertTrue(len(blocks), 2)
        # call to DBS should be resovled within 1 second
        self.assertTrue(elapsedTime < 3)

        time0 = time.time()
        blockInfoList = getBlockInfo4PU(blocks, self.ckey, self.cert)
        for blk, row in blockInfoList.items():
            self.logger.debug("block %s, nfiles=%d, nevents=%d", blk, len(row['FileList']), row['NumberOfEvents'])
        elapsedTime = time.time() - time0
        self.logger.debug("Elapsed time: %d seconds", elapsedTime)
        # NOTE: if every DBS call spent 1 second, avg time for 10 calls will be around 1 second
        # therefore, we will test quite low number here
        self.assertTrue(elapsedTime < 3)


if __name__ == '__main__':
    unittest.main()
