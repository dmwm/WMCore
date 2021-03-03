#!/usr/bin/env python

from __future__ import (division, print_function)

import unittest
import logging
from dbs.apis.dbsClient import DbsApi

from Utils.ExtendedUnitTestCase import ExtendedUnitTestCase
from WMQuality.Emulators.DBSClient.MockDbsApi import MockDbsApi

# a small dataset that should always exist
DATASET = '/HighPileUp/Run2011A-v1/RAW'
BLOCK = '/HighPileUp/Run2011A-v1/RAW#fabf118a-cbbf-11e0-80a9-003048caaace'
FILE_NAMES = [u'/store/data/Commissioning2015/Cosmics/RAW/v1/000/238/545/00000/C47FDF25-2ECF-E411-A8E2-02163E011839.root']


class MockDbsApiTest(ExtendedUnitTestCase):
    """
    Class that can be imported to switch to 'mock'ed versions of
    services.

    """

    def setUp(self):
        self.endpoint = 'https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'
        self.realDBS = DbsApi(self.endpoint)
        self.mockDBS = MockDbsApi(self.endpoint)
        return

    def tearDown(self):
        return

    def testDBSApiMembers(self):
        """
        Tests members from DBSApi
        """

        block_with_parent = '/Cosmics/Commissioning2015-PromptReco-v1/RECO#004ac3ba-d09e-11e4-afad-001e67ac06a0'
        # List of members from DBSApi
        members = {'listDataTiers': {},
                   'listDatasets': {'data_tier_name': 'RAW', 'primary_ds_name': 'Jet'},
                   'listFileLumis': {'block_name': BLOCK, 'validFileOnly': 1},
                   # 'listFileLumiArray':{'logical_file_name': FILE_NAMES},
                   'listFileParents': {'block_name': block_with_parent},
                   'listPrimaryDatasets': {'primary_ds_name': 'Jet*'},
                   'listRuns': {'dataset': DATASET},
                   'listFileArray': {'dataset': DATASET, 'detail': True, 'validFileOnly': 1},
                   'listFileSummaries': {'dataset': DATASET, 'validFileOnly': 1},
                   'listBlocks': {'dataset': DATASET, 'detail': True},
                   'listBlockParents': {'block_name': block_with_parent},
                   }

        for member in list(members.keys()):
            # Get from  mock DBS
            args = []
            kwargs = members[member]
            logging.info("Querying API: %s, with parameters: %s", member, members[member])
            real = getattr(self.realDBS, member)(*args, **kwargs)
            mock = getattr(self.mockDBS, member)(*args, **kwargs)

            self.assertContentsEqual(real, mock, "Identity check on member %s failed" % member)

        return


if __name__ == '__main__':
    unittest.main()
