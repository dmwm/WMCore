import unittest
from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList, removePasswordFromUrl

class ReqMgrWebToolsTest(unittest.TestCase):
    def testParseRunList(self):
        l0 = ''
        l1 = ' [1,  2,  3 ] '
        l2 = '1,  2, 3   '
        l3 = u'1,  2, 3   '
        l4 = [1,2,3]
        l5 = {1:2, 3:4}
        self.assertEqual(parseRunList(l0), [])
        self.assertEqual(parseRunList(l1), [1,2,3])
        self.assertEqual(parseRunList(l2), [1,2,3])
        self.assertEqual(parseRunList(l3), [1,2,3])
        self.assertEqual(parseRunList(l4), [1,2,3])
  
    def testParseBlockList(self):
        l3 = '  ["Barack", "  Sarah  ",George]'
        self.assertEqual(parseBlockList(l3), ['Barack', 'Sarah', 'George'])

    def testRemovePassword(self):
        url = 'http://sarah:maverick@whitehouse.gov:1600/birthcertificates/trig'
        cleanedUrl = removePasswordFromUrl(url)
        self.assertEqual(cleanedUrl, 'http://whitehouse.gov:1600/birthcertificates/trig')

