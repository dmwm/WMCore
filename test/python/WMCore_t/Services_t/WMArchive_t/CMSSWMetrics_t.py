import unittest
from WMCore.Services.WMArchive.CMSSWMetrics import CMSSWMetrics, CMSSW_METRICS


class CMSSWMetrics_t(unittest.TestCase):
    """
    Unit test class for CMSSW metrics module
    """

    def testCMSSWMetrics(self):
        """
        unit test for CMSSWMetrics function
        """
        tdict = CMSSWMetrics()
        for key, tdict in tdict.items():
            vdict = CMSSW_METRICS[key]
            if key != 'XrdSiteStatistics':
                for tkey, val in tdict.items():
                    value = vdict[tkey]
                    self.assertTrue(val == type(value))

    def testXrdMetrics(self):
        """
        unit test for XrdMetrics function
        """
        tdict = CMSSWMetrics()['XrdSiteStatistics']
        for key, xarr in tdict.items():
            varr = CMSSW_METRICS['XrdSiteStatistics'][key]
            # loop over each dict in specific metrics array
            for tdict in varr:
                for tkey, val in tdict.items():
                    etype = xarr[0][tkey]
                    # special case when expected type is float but value is 0 which interpreted as int
                    if val == 0 and etype == float:
                        continue
                    self.assertTrue(type(val) == etype)


if __name__ == '__main__':
    unittest.main()
