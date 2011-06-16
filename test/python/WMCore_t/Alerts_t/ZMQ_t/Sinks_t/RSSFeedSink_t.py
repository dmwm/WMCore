import os
import unittest
import time
from WMCore.Configuration import ConfigSection
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.RSSFeedSink import RSSFeedSink



class RSSFeedSinkTest(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection("rss")
        self.config.outputfile = "/tmp/RSSFeedSinkTest-rss.xml"
        self.config.linkBase = "https://localhost/WMAgent/"
        self.config.depth = 10
    
    
    def tearDown(self):
        if os.path.exists(self.config.outputfile):
            os.remove(self.config.outputfile)
            
    
    def _getAlerts(self, nAlerts):
        alerts = []
        for i in range(nAlerts):
            # call time.time() doesn't give very high time precision, add artificial
            # delay so that consecutive alerts do have different Timestamp which
            # is possible to compare  
            a = Alert(Source = __file__, Level = i, Timestamp = time.time(),
                      Type = "Test", Component = self.__class__.__name__)
            alerts.append(a)
            time.sleep(0.05)
        return alerts
        
    
    def testRSSFeedSinkBasicTest(self):
        nAlerts = 5
        sink = RSSFeedSink(self.config)
        alerts = self._getAlerts(nAlerts)        
            
        sink.send(alerts)
        
        # test by reading back
        rootElem = sink.load()
        # test one of the pre-items elements
        link = rootElem.iterchildren(tag = "channel").next().iterchildren(tag = "link").next()
        self.assertEqual(link.text, self.config.link)
        # test items
        items = rootElem.iterchildren(tag = "channel").next().iterchildren(tag = "item")
        # alerts as generated above were put in the feed in reverse order (so
        # that most recent appear first), thus Levels are 4, 3, 2 ...
        timeNow = time.time() # now
        for level, item in zip(sorted(range(nAlerts), reverse = True), items):
            levelElem = item.iterchildren(tag = "Level").next()
            self.assertEqual(int(levelElem.text), level)
            timeStamp = item.iterchildren(tag = "Timestamp").next()
            # text items have descending order, the most recent appear first
            self.assertTrue(timeNow > float(timeStamp.text))
            timeNow = timeStamp.text
            
            
    def _testMultipleSending(self, nFirstRound = 0, nSecondRound = 0, depth = 0, totalExpected = 0):
        self.config.depth = depth
        sink = RSSFeedSink(self.config)
        alerts = self._getAlerts(nFirstRound)
        sink.send(alerts)

        # new round
        alerts = self._getAlerts(nSecondRound)
        sink.send(alerts)
        
        rootElem = sink.load()
        # test items
        items = rootElem.iterchildren(tag = "channel").next().iterchildren(tag = "item")
        timeNow = time.time() # now
        count = 1
        for item in items:
            timeStamp = item.iterchildren(tag = "Timestamp").next()
            # text items have descending order, the most recent appear first
            self.assertTrue(timeNow > float(timeStamp.text))
            timeNow = timeStamp.text
            count += 1
        self.assertTrue(count, totalExpected)
            

    def testRSSFeedSinkCombineNewAndOldAlertsFromFile(self):
        self._testMultipleSending(nFirstRound = 5, nSecondRound = 5, depth = 8, totalExpected = 8)
        

    def testRSSFeedSinkCombineNewAndOldNoNewAdded(self):
        self._testMultipleSending(nFirstRound = 5, nSecondRound = 5, depth = 5, totalExpected = 5)
            
        
    def testRSSFeedSinkCombineHighDepth(self):
        self._testMultipleSending(nFirstRound = 20, nSecondRound = 10, depth = 40, totalExpected = 30)
        


if __name__ == "__main__":
    unittest.main()