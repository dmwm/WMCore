"""
_ContinuousSummaryHistogram_t_

Unit test module for the ContinuousSummaryHistogram module.
It checks the sanity of the histogram constructions,
corner cases and its compatibility with JSON data published
to couch.

Created on Nov 20, 2012

@author: dballest
"""
from __future__ import division
from builtins import range
from future.utils import viewvalues

import os
import unittest
import random

from WMCore.DataStructs.MathStructs.ContinuousSummaryHistogram import ContinuousSummaryHistogram
from WMCore.Database.CMSCouch import Database
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class ContinuousSummaryHistogramTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_

        Setup a couch database for testing
        of produced JSON
        """

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setupCouch("histogram_dump_t")
        random.seed()
        self.histogramDB = Database(url=os.getenv("COUCHURL"), dbname="histogram_dump_t")

    def tearDown(self):
        """
        _tearDown_

        Clean the couch
        """
        self.testInit.tearDownCouch()

    def buildRandomNumberList(self, n, distribution = "normalvariate", **kwargs):
        """
        _buildRandomNumberList_

        Builds a list with n pseudorandomly distributed
        numbers according to some given distribution
        """
        numberList = []
        if not kwargs:
            kwargs = {"mu" : 0, "sigma" : 1}
        for _ in range(n):
            generator = getattr(random, distribution)
            numberList.append(generator(**kwargs))

        return numberList

    def testA_BasicTest(self):
        """
        _testA_BasicTest_

        Build a histogram from a set of uniformly
        distributed pseudorandom numbers. Check
        that the statistic properties
        in the histogram are accurate to some degree,
        that the histogram binning is done right and
        that this can become a document an uploaded to couch
        """
        inputData = self.buildRandomNumberList(1000)

        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel')

        # Populate the histogram
        for point in inputData:
            histogram.addPoint(point)

        # Get the JSON
        jsonHistogram = histogram.toJSON()

        # Check the histogram core data
        self.assertEqual(jsonHistogram["title"], "TestHisto")
        self.assertEqual(jsonHistogram["xLabel"], "MyLabel")
        self.assertAlmostEqual(jsonHistogram["average"], 0.0, places = 0)
        self.assertAlmostEqual(jsonHistogram["stdDev"], 1.0, places = 0)
        self.assertEqual(len(jsonHistogram["data"]), 16)
        self.assertTrue(jsonHistogram["continuous"])

        # Check the internal data
        self.assertEqual(jsonHistogram["internalData"]["yLabel"], "SomeoneElsesLabel")
        self.assertEqual(jsonHistogram["internalData"]["nPoints"], 1000)

        # Try to commit it to couch
        jsonHistogram["_id"] = jsonHistogram["title"]
        self.histogramDB.commitOne(jsonHistogram)

        storedJSON = self.histogramDB.document("TestHisto")
        self.assertEqual(len(storedJSON["data"]), 16)

        return

    def testB_extremeData(self):
        """
        _testB_extremeData_

        Put extreme points in the data and try to build a histogram.
        Check that it can process all this correctly
        """

        # First no data
        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel')
        jsonHistogram = histogram.toJSON()
        self.assertEqual(jsonHistogram["title"], "TestHisto")
        self.assertEqual(jsonHistogram["xLabel"], "MyLabel")
        self.assertEqual(jsonHistogram["average"], 0.0)
        self.assertEqual(jsonHistogram["stdDev"], 0.0)
        self.assertEqual(len(jsonHistogram["data"]), 0)

        # Data with NaNs and Infs
        inputData = self.buildRandomNumberList(100)
        inputData.append(float('NaN'))
        inputData.append(float('Inf'))
        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel')
        for point in inputData:
            histogram.addPoint(point)
        jsonHistogram = histogram.toJSON()
        self.assertAlmostEqual(jsonHistogram["average"], 0.0, places = 0)
        self.assertAlmostEqual(jsonHistogram["stdDev"], 1.0, places = 0)
        self.assertEqual(len(jsonHistogram["data"]), 7)
        self.assertEqual(jsonHistogram["internalData"]["nPoints"], 100)

        # One single point, P5
        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel')
        histogram.addPoint(5)
        jsonHistogram = histogram.toJSON()
        self.assertEqual(jsonHistogram["average"], 5.0)
        self.assertEqual(jsonHistogram["stdDev"], 0.0)
        self.assertEqual(len(jsonHistogram["data"]), 1)
        self.assertEqual(jsonHistogram["data"]["5.0,5.0"], 1)
        self.assertEqual(jsonHistogram["internalData"]["nPoints"], 1)

        # Test that toJSON is idempotent
        inputData = self.buildRandomNumberList(100)
        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel')
        for point in inputData:
            histogram.addPoint(point)
        jsonHistogram = histogram.toJSON()
        oldData = jsonHistogram["data"]
        jsonHistogram = histogram.toJSON()
        self.assertAlmostEqual(jsonHistogram["average"], 0.0, places = 0)
        self.assertAlmostEqual(jsonHistogram["stdDev"], 1.0, places = 0)
        self.assertEqual(len(jsonHistogram["data"]), 7)
        self.assertEqual(jsonHistogram["data"], oldData)
        self.assertEqual(jsonHistogram["internalData"]["nPoints"], 100)

        return

    def testC_compactHistogram(self):
        """
        _testC_compactHistogram_

        Check that we can create smaller histograms objects
        by chopping outliers and dropping the data all together
        """

        # Input normally distributed data and chop anything above 1 stdev (32% of data)
        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel',
                                               dropOutliers = True, sigmaLimit = 1)
        inputData = self.buildRandomNumberList(1000)
        for point in inputData:
            histogram.addPoint(point)
        jsonHistogram = histogram.toJSON()
        self.assertAlmostEqual(jsonHistogram["average"], 0.0, places = 0)
        self.assertAlmostEqual(jsonHistogram["stdDev"], 1.0, places = 0)
        self.assertEqual(len(jsonHistogram["data"]), 16)
        self.assertEqual(jsonHistogram["internalData"]["nPoints"], 1000)
        pointsInHistogram = sum([x for x in viewvalues(jsonHistogram["data"])])

        # With high probability we must have chopped at least one point
        self.assertTrue(pointsInHistogram < 1000)
        self.assertAlmostEqual(pointsInHistogram / 1000.0, 0.68, places = 1)

        # Create a histogram without histogram data
        histogram = ContinuousSummaryHistogram('TestHisto', 'MyLabel', 'SomeoneElsesLabel',
                                               storeHistogram = False)
        inputData = self.buildRandomNumberList(1000)
        for point in inputData:
            histogram.addPoint(point)
        jsonHistogram = histogram.toJSON()
        self.assertAlmostEqual(jsonHistogram["average"], 0.0, places = 0)
        self.assertAlmostEqual(jsonHistogram["stdDev"], 1.0, places = 0)
        self.assertEqual(len(jsonHistogram["data"]), 0)
        self.assertEqual(jsonHistogram["internalData"]["nPoints"], 1000)

        return

if __name__ == "__main__":
    unittest.main()
