"""
_DiscreteSummaryHistogram_t_

Unit test module for the DiscreteSummaryHistogram module.
It checks the sanity of the histogram constructions,
corner cases and its compatibility with JSON data published
to couch.

Created on Nov 20, 2012

@author: dballest
"""

from builtins import range
import unittest
import os

from WMCore.DataStructs.MathStructs.DiscreteSummaryHistogram import DiscreteSummaryHistogram
from WMCore.Database.CMSCouch import Database
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class DiscreteSummaryHistogramTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup a couch database for testing
        of produced JSON
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setupCouch("histogram_dump_t")
        self.histogramDB = Database(url=os.getenv("COUCHURL") ,dbname = "histogram_dump_t")

    def tearDown(self):
        """
        _tearDown_

        Clean the couch
        """
        self.testInit.tearDownCouch()

    def testA_BasicTest(self):
        """
        _testA_BasicTest_

        Build a histogram from a set of discrete data. Check
        that the statistic properties in the histogram are accurate,
        and that this can become a document an uploaded to couch
        """
        # Try and empty one
        histogram = DiscreteSummaryHistogram('SomeTitle', 'Categories')
        histogramJSON = histogram.toJSON()

        self.assertEqual(histogramJSON["title"], "SomeTitle")
        self.assertEqual(histogramJSON["xLabel"], "Categories")
        self.assertFalse(histogramJSON["continuous"])
        self.assertEqual(len(histogramJSON["data"]), 0)
        self.assertEqual(histogramJSON["average"], {})
        self.assertEqual(histogramJSON["stdDev"], {})

        histogram = DiscreteSummaryHistogram('SomeTitle', 'Categories')

        for _ in range(5):
            histogram.addPoint("CategoryA", "FeatureA")
            histogram.addPoint("CategoryB", "FeatureB")

        for _ in range(17):
            histogram.addPoint("CategoryA", "FeatureB")
            histogram.addPoint("CategoryC", "FeatureB")

        for _ in range(3):
            histogram.addPoint("CategoryC", "FeatureA")

        jsonHistogram = histogram.toJSON()

        # Average/stdDev per feature:
        # FeatureA: avg = 2.7 stdev = 2.05
        # FeatureB: avg = 13 stdev = 5.66
        self.assertAlmostEqual(jsonHistogram["average"]["FeatureA"], 2.7, places = 1)
        self.assertAlmostEqual(jsonHistogram["average"]["FeatureB"], 13, places = 1)
        self.assertAlmostEqual(jsonHistogram["stdDev"]["FeatureA"], 2.05, places = 1)
        self.assertAlmostEqual(jsonHistogram["stdDev"]["FeatureB"], 5.66, places = 1)
        self.assertEqual(jsonHistogram["data"]["CategoryA"]["FeatureA"], 5)
        self.assertEqual(jsonHistogram["data"]["CategoryA"]["FeatureB"], 17)
        self.assertEqual(jsonHistogram["data"]["CategoryB"]["FeatureA"], 0)
        self.assertEqual(jsonHistogram["data"]["CategoryB"]["FeatureB"], 5)
        self.assertEqual(jsonHistogram["data"]["CategoryC"]["FeatureA"], 3)
        self.assertEqual(jsonHistogram["data"]["CategoryC"]["FeatureB"], 17)

        # Test couch
        # Try to commit it to couch
        jsonHistogram["_id"] = jsonHistogram["title"]
        self.histogramDB.commitOne(jsonHistogram)

        storedJSON = self.histogramDB.document("SomeTitle")
        self.assertEqual(len(storedJSON["data"]), 3)

        return

if __name__ == "__main__":
    unittest.main()
