"""
_ContinuousSummaryHistogram_

Continuous histogram module, to be used by the TaskArchiver
to store histograms in the summary where the data is a continuous number.

Created on Nov 20, 2012

@author: dballest
"""
from __future__ import division
import math

from WMCore.DataStructs.MathStructs.SummaryHistogram import SummaryHistogram
from WMCore.Algorithms.MathAlgos import validateNumericInput
from WMCore.Algorithms.MathAlgos import calculateRunningAverageAndQValue, calculateStdDevFromQ

class ContinuousSummaryHistogram(SummaryHistogram):
    """
    _ContinuousSummaryHistogram_

    A histogram where there are continuous points
    with certain frequency, it follows
    that there is only one value in Y and
    that the average and standard deviation are
    not calculated on the frequency values but the X values.
    """

    def __init__(self, title, xLabel, yLabel = None,
                 roundingDecimals = 2, nBins = None,
                 dropOutliers = False, sigmaLimit = 5,
                 storeHistogram = True):
        """
        __init__

        Initialize a more complex histogram structure, containing different
        data to calculate online average and standard deviations. This data is also
        stored in the JSON to allow rebuilding and adding histograms.

        All histograms are binned when requested, the resolution can be specified
        through nBins, otherwise the value used is the one recommended in:
        Wand, M.P. (1997), "Data-Based Choice of Histogram Bin Width," The American Statistician, 51, 59-64.

        If specified, outlier farther than sigmaLimit standard deviations from the
        mean will not be included in the binned histogram.
        """
        # Initialize the parent object
        SummaryHistogram.__init__(self, title, xLabel)

        # Indicate this is a discrete histogram
        self.continuous = True

        # Add data only used in the continuous version
        self.yLabel            = yLabel
        self.nPoints           = 0
        self.QValue            = None
        self.average           = None

        # Configuration parameters for the continuous histograms
        self.roundingDecimals = roundingDecimals
        self.fixedNBins       = nBins
        self.dropOutliers     = dropOutliers
        self.sigmaLimit       = sigmaLimit
        self.binned           = False
        self.storeHistogram   = storeHistogram

        # Override initialization of some attributes
        self.average = 0.0
        self.stdDev  = 0.0

        return

    def addPoint(self, xValue, yLabel = None):
        """
        _addPoint_

        Add a point from a continuous set (only-numbers allowed currently) to the histogram data,
        calculate the running average and standard deviation.
        If no y-label had been specified before, one must be supplied
        otherwise the given y-label must be either None or equal
        to the stored value.
        """
        if self.binned:
            # Points can't be added to binned histograms!
            raise Exception("Points can't be added to binned histograms")

        if self.yLabel is None and yLabel is None:
            raise Exception("Some y-label must be stored for the histogram")
        elif self.yLabel is None:
            self.yLabel = yLabel
        elif yLabel is not None and self.yLabel != yLabel:
            raise Exception("Only one y-label is allowed on continuous histograms")

        if not validateNumericInput(xValue):
            # Do nothing if it is not a number
            return

        xValue = float(xValue)
        xValue = round(xValue, self.roundingDecimals)

        if self.storeHistogram:
            if xValue not in self.data:
                self.data[xValue] = 0
            self.data[xValue] += 1

        self.nPoints += 1

        (self.average, self.QValue) = calculateRunningAverageAndQValue(xValue, self.nPoints, self.average, self.QValue)

        return

    def __add__(self, other):
        #TODO: For HG1302, support multiple agents properly in the workload summary
        raise NotImplementedError

    def toJSON(self):
        """
        _toJSON_

        Bin the histogram if any, calculate the standard deviation. Store
        the internal data needed for reconstruction of the histogram
        from JSON and call superclass toJSON method.
        """
        if self.nPoints:
            self.stdDev = calculateStdDevFromQ(self.QValue, self.nPoints)
        if not self.binned and self.storeHistogram:
            self.binHistogram()
        self.jsonInternal = {}
        self.jsonInternal['yLabel'] = self.yLabel
        self.jsonInternal['QValue'] = self.QValue
        self.jsonInternal['nPoints'] = self.nPoints
        return SummaryHistogram.toJSON(self)

    def binHistogram(self):
        """
        _binHistogram_

        Histograms of continuous data must be binned,
        this takes care of that using given or optimal parameters.
        Note that this modifies the data object,
        and points can't be added to the histogram after this.
        """

        if not self.nPoints:
            return

        self.binned = True

        # Number of bins can be specified or calculated based on number of points
        nBins = self.fixedNBins
        if nBins is None:
            nBins = int(math.floor((5.0 / 3.0) * math.pow(self.nPoints, 1.0 / 3.0)))

        # Define min and max
        if not self.dropOutliers:
            upperLimit = max(self.data.keys())
            lowerLimit = min(self.data.keys())
        else:
            stdDev = calculateStdDevFromQ(self.QValue, self.nPoints)
            upperLimit = self.average + (stdDev * self.sigmaLimit)
            lowerLimit = self.average - (stdDev * self.sigmaLimit)

        # Incremental delta
        delta = abs(float(upperLimit - lowerLimit)) / nBins

        # Build the bins, it's a list of tuples for now
        bins = []

        a = lowerLimit
        b = lowerLimit + delta
        while len(bins) < nBins:
            bins.append((a, b))
            a += delta
            b += delta
        # Go through data and populate the binned histogram
        binnedHisto = {}
        currentBin = 0
        currentPoint = 0
        sortedData = sorted(self.data.keys())
        while currentPoint < len(sortedData):
            point = sortedData[currentPoint]
            encodedTuple = "%s,%s" % (bins[currentBin][0], bins[currentBin][1])
            if encodedTuple not in binnedHisto:
                binnedHisto[encodedTuple] = 0
            if point > upperLimit or point < lowerLimit:
                currentPoint += 1
            elif currentBin == len(bins) - 1:
                binnedHisto[encodedTuple] += self.data[point]
                currentPoint += 1
            elif point >= bins[currentBin][0] and point < bins[currentBin][1]:
                binnedHisto[encodedTuple] += self.data[point]
                currentPoint += 1
            else:
                currentBin += 1

        self.data = binnedHisto
        return
