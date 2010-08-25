#!/usr/bin/env python
"""
    WorkQueue.Policy.Start.Dataset tests
"""

__revision__ = "$Id: Dataset_t.py,v 1.2 2009/12/14 13:56:40 swakef Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import shutil
from WMCore.WorkQueue.Policy.Start.Dataset import Dataset
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workload as Tier1ReRecoWorkload
from WMCore_t.WMSpec_t.samples.Tier1ReRecoWorkload import workingDir
from WMCore_t.WorkQueue_t.MockDBSReader import MockDBSReader
shutil.rmtree(workingDir, ignore_errors = True)

class DatasetTestCase(unittest.TestCase):

    splitArgs = dict(SliceType = 'number_of_files', SliceSize = 10)

    def testTier1ReRecoWorkload(self):
        """Tier1 Re-reco workflow"""
        dbs_endpoint = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
        dbs = MockDBSReader(dbs_endpoint,
                            '/Cosmics/CRAFT09-PromptReco-v1/RECO')
        dbs = {dbs_endpoint : dbs}
        inputDataset = Tier1ReRecoWorkload.taskIterator().next().inputDataset()
        dataset = "/%s/%s/%s" % (inputDataset.primary,
                                     inputDataset.processed,
                                     inputDataset.tier)
        units = Dataset(**self.splitArgs)(Tier1ReRecoWorkload, dbs)
        self.assertEqual(1, len(units))
        for unit in units:
            self.assertEqual(2, unit['Jobs'])
            spec = unit['WMSpec']
            initialTask = spec.taskIterator().next()
            self.assertEqual(unit['Data'], dataset)


if __name__ == '__main__':
    unittest.main()
