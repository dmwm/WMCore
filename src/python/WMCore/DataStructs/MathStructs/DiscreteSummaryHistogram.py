"""
_DiscreteSummaryHistogram_

Discrete histogram module, to be used by the TaskArchiver
to store histograms in the summary where the data is categorical.

Created on Nov 20, 2012

@author: dballest
"""

from WMCore.DataStructs.MathStructs.SummaryHistogram import SummaryHistogram
from WMCore.Algorithms.MathAlgos import getAverageStdDev

class DiscreteSummaryHistogram(SummaryHistogram):
    """
    _DiscreteSummaryHistogram_

    A histogram where the data is organized by
    a finite number of categories, it can have
    many values for each category.
    """


    def __init__(self, title, xLabel):
        """
        __init__

        Initialize a simpler histogram that only stores
        the histogram. Everything else is calculated when the JSON is requested.
        """
        # Initialize the parent object
        SummaryHistogram.__init__(self, title, xLabel)

        # Indicate this is a discrete histogram
        self.continuous = False

        # Add data only used in the discrete version
        self.yLabels    = set()

        # Override initialization of some attributes
        self.average = {}
        self.stdDev  = {}

        return

    def addPoint(self, xValue, yLabel):
        """
        _addPoint_

        Add point to discrete histogram,
        x value is a category and therefore not rounded.
        There can be many yLabel and standard deviations are
        not calculated online. Histograms are always stored.
        """
        if xValue not in self.data:
            # Record the category
            self.data[xValue] = {}
            for label in self.yLabels:
                self.data[xValue][label] = 0

        if yLabel not in self.yLabels:
            # Record the label
            self.yLabels.add(yLabel)
            self.average[yLabel] = 0.0
            self.stdDev[yLabel]  = 0.0
            for category in self.data:
                self.data[category][yLabel] = 0

        self.data[xValue][yLabel] += 1

        return

    def __add__(self, other):
        #TODO: For HG1302, support multiple agents properly in the workload summary
        raise NotImplementedError

    def toJSON(self):
        """
        _toJSON_

        Calculate average and standard deviation, store it
        and call the parent class toJSON method
        """

        for yLabel in self.yLabels:
            numList = []
            for xValue in self.data:
                numList.append(self.data[xValue][yLabel])
            (self.average[yLabel], self.stdDev[yLabel]) = getAverageStdDev(numList)

        return SummaryHistogram.toJSON(self)
