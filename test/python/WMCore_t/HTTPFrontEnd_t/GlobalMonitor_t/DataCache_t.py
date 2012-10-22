import unittest
from WMCore.HTTPFrontEnd.GlobalMonitor.DataCache import DataCache

class DataCacheTest(unittest.TestCase):

    def setUp(self):
        self.dataCache = DataCache()

    def testDataCache(self):
        agentData = ['agentData1']
        self.dataCache.setAgentData(agentData)
        self.assertEqual(self.dataCache.isAgentDataExpired(), False)
        self.assertEqual(self.dataCache.getAgentData(), agentData)

        requestData = ['requestData1']
        self.dataCache.setRequestData(requestData)
        self.assertEqual(self.dataCache.isRequestDataExpired(), False)
        self.assertEqual(self.dataCache.getRequestData(), requestData)

        siteData = ['siteData1']
        self.dataCache.setSiteData(siteData)
        self.assertEqual(self.dataCache.isSiteDataExpired(), False)
        self.assertEqual(self.dataCache.getSiteData(), siteData)

        self.dataCache.setDuration(-1)
        self.assertEqual(self.dataCache.isAgentDataExpired(), True)
        self.assertEqual(self.dataCache.isRequestDataExpired(), True)
        self.assertEqual(self.dataCache.isSiteDataExpired(), True)

if __name__ == '__main__':
    unittest.main()
