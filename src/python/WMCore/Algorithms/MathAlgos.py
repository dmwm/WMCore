#!/usr/bin/env

"""
_MathAlgos_

Simple mathematical tools and tricks that might prove to
be useful.
"""
from __future__ import print_function, division
import math
import decimal
import logging

from WMCore.WMException import WMException


class MathAlgoException(WMException):
    """
    Some simple math algo exceptions

    """
    pass


def getAverageStdDev(numList):
    """
    _getAverageStdDev_

    Given a list, calculate both the average and the
    standard deviation.
    """

    if len(numList) < 0:
        # Nothing to do here
        return 0.0, 0.0

    total   = 0.0
    average = 0.0
    stdBase = 0.0

    # Assemble the average
    skipped = 0
    for value in numList:
        try:
            if math.isnan(value) or math.isinf(value):
                skipped += 1
                continue
            else:
                total += value
        except TypeError:
            msg =  "Attempted to take average of non-numerical values.\n"
            msg += "Expected int or float, got %s: %s" % (value.__class__, value)
            logging.error(msg)
            logging.debug("FullList: %s" % numList)
            raise MathAlgoException(msg)

    length = len(numList) - skipped
    if length < 1:
        return average, total

    average = total / length

    for value in numList:
        tmpValue = value - average
        stdBase += (tmpValue * tmpValue)

    stdDev = math.sqrt(stdBase / length)

    if math.isnan(average) or math.isinf(average):
        average = 0.0
    if math.isnan(stdDev) or math.isinf(average) or not decimal.Decimal(str(stdDev)).is_finite():
        stdDev = 0.0
    if not isinstance(stdDev, (int, float)):
        stdDev = 0.0

    return average, stdDev


def createHistogram(numList, nBins, limit):
    """
    _createHistogram_

    Create a histogram proxy (a list of bins) for a
    given list of numbers
    """

    average, stdDev = getAverageStdDev(numList = numList)

    underflow  = []
    overflow   = []
    histEvents = []
    histogram  = []
    for value in numList:
        if math.fabs(average - value) <= limit * stdDev:
            # Then we counted this event
            histEvents.append(value)
        elif average < value:
            overflow.append(value)
        elif average > value:
            underflow.append(value)

    if len(underflow) > 0:
        binAvg, binStdDev = getAverageStdDev(numList=underflow)
        histogram.append({'type': 'underflow',
                          'average': binAvg,
                          'stdDev': binStdDev,
                          'nEvents': len(underflow)})
    if len(overflow) > 0:
        binAvg, binStdDev = getAverageStdDev(numList=overflow)
        histogram.append({'type': 'overflow',
                          'average': binAvg,
                          'stdDev': binStdDev,
                          'nEvents': len(overflow)})
    if len(histEvents) < 1:
        # Nothing to do?
        return histogram

    histEvents.sort()
    upperBound = max(histEvents)
    lowerBound = min(histEvents)
    if lowerBound == upperBound:
        # This is a problem
        logging.debug("Only one value in the histogram!")
        nBins = 1
        upperBound = upperBound + 1
        lowerBound = lowerBound - 1
    binSize = (upperBound - lowerBound)/nBins
    binSize = floorTruncate(binSize)

    for x in range(nBins):
        lowerEdge = floorTruncate(lowerBound + (x * binSize))
        histogram.append({'type': 'standard',
                          'lowerEdge': lowerEdge,
                          'upperEdge': lowerEdge + binSize,
                          'average': 0.0,
                          'stdDev': 0.0,
                          'nEvents': 0})

    for bin in histogram:
        if bin['type'] != 'standard':
            continue
        binList = []
        for value in histEvents:
            if value >= bin['lowerEdge'] and value <= bin['upperEdge']:
                # Then we're in the bin
                binList.append(value)
            elif value > bin['upperEdge']:
                # Because this is a sorted list we are now out of the bin range
                # Calculate our values and break
                break
            else:
                continue

        # If we get here, it's because we're out of values in the bin
        # Time to do some math
        if len(binList) < 1:
            # Nothing to do here, leave defaults
            continue

        binAvg, binStdDev = getAverageStdDev(numList=binList)
        bin['average'] = binAvg
        bin['stdDev']  = binStdDev
        bin['nEvents'] = len(binList)

    return histogram


def floorTruncate(value, precision=3):
    """
    _floorTruncate_

    Truncate a value to a set number of decimal points

    Always truncates to a LOWER value, this is so that using it for
    histogram binning creates values beneath the histogram lower edge.
    """
    prec = math.pow(10, precision)

    return math.floor(value * prec)/prec


def sortDictionaryListByKey(dictList, key, reverse=False):
    """
    _sortDictionaryListByKey_

    Given a list of dictionaries and a key with a numerical
    value, sort that dictionary in order of that key's value.

    NOTE: If the key does not exist, this will not raise an exception
    This is because this is used for sorting of performance histograms
    And not all histograms have the same value
    """

    return sorted(dictList, key=lambda k: k.get(key, 0.0), reverse=reverse)


def getLargestValues(dictList, key, n=1):
    """
    _getLargestValues_

    Take a list of dictionaries, sort them by the value of a
    particular key, and return the n largest entries.

    Key must be a numerical key.
    """

    sortedList = sortDictionaryListByKey(dictList=dictList, key=key, reverse=True)

    return sortedList[:n]


def validateNumericInput(value):
    """
    _validateNumericInput_

    Check that the value is actually an usable number
    """
    value = float(value)
    try:
        if math.isnan(value) or math.isinf(value):
            return False
    except TypeError:
        return False

    return True


def calculateRunningAverageAndQValue(newPoint, n, oldM, oldQ):
    """
    _calculateRunningAverageAndQValue_

    Use the algorithm described in:
    Donald E. Knuth (1998). The Art of Computer Programming, volume 2: Seminumerical Algorithms, 3rd ed.., p. 232. Boston: Addison-Wesley.

    To calculate an average and standard deviation while getting data, the standard deviation
    can be obtained from the so-called Q value with the following equation:

    sigma = sqrt(Q/n)

    This is also contained in the function calculateStdDevFromQ in this module. The average is equal to M.
    """

    if not validateNumericInput(newPoint): raise MathAlgoException("Provided a non-valid newPoint")
    if not validateNumericInput(n): raise MathAlgoException("Provided a non-valid n")

    if n == 1:
        M = newPoint
        Q = 0.0
    else:
        if not validateNumericInput(oldM): raise MathAlgoException("Provided a non-valid oldM")
        if not validateNumericInput(oldQ): raise MathAlgoException("Provided a non-valid oldQ")
        M = oldM + (newPoint - oldM) / n
        Q = oldQ + ((n - 1) * (newPoint - oldM) * (newPoint - oldM) / n)

    return M, Q


def calculateStdDevFromQ(Q, n):
    """
    _calculateStdDevFromQ_

    If Q is the sum of the squared differences of some points to their average,
    then the standard deviation is given by:

    sigma = sqrt(Q/n)

    This function calculates that formula
    """
    if not validateNumericInput(Q): raise MathAlgoException("Provided a non-valid Q")
    if not validateNumericInput(n): raise MathAlgoException("Provided a non-valid n")

    sigma = math.sqrt(Q / n)

    if not validateNumericInput(sigma): return 0.0

    return sigma
