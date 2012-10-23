#!/usr/bin/env

"""
_MathAlgos_

Simple mathematical tools and tricks that might prove to
be useful.
"""

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
    stdDev  = 0.0
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

    average = float(total)/length

    for value in numList:
        tmpValue = value - average
        stdBase += (tmpValue * tmpValue)

    stdDev = math.sqrt(stdBase/length)

    if math.isnan(average) or math.isinf(average):
        average = 0.0
    if math.isnan(stdDev) or math.isinf(average) or not decimal.Decimal(str(stdDev)).is_finite():
        stdDev = 0.0
    if type(stdDev) != float and type(stdDev) != int:
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
        binAvg, binStdDev = getAverageStdDev(numList = underflow)
        histogram.append({'type': 'underflow',
                          'average': binAvg,
                          'stdDev': binStdDev,
                          'nEvents': len(underflow)})
    if len(overflow) > 0:
        binAvg, binStdDev = getAverageStdDev(numList = overflow)
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
        logging.error("Only one value in the histogram!")
        nBins = 1
        upperBound = upperBound + 1
        lowerBound = lowerBound - 1
    binSize = float(upperBound - lowerBound)/nBins
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

        binAvg, binStdDev = getAverageStdDev(numList = binList)
        bin['average'] = binAvg
        bin['stdDev']  = binStdDev
        bin['nEvents'] = len(binList)





    return histogram


def floorTruncate(value, precision = 3):
    """
    _floorTruncate_

    Truncate a value to a set number of decimal points

    Always truncates to a LOWER value, this is so that using it for
    histogram binning creates values beneath the histogram lower edge.
    """

    prec = math.pow(10, precision)

    return math.floor(float(value * prec))/float(prec)


def sortDictionaryListByKey(dictList, key, reverse = False):
    """
    _sortDictionaryListByKey_

    Given a list of dictionaries and a key with a numerical
    value, sort that dictionary in order of that key's value.

    NOTE: If the key does not exist, this will not raise an exception
    This is because this is used for sorting of performance histograms
    And not all histograms have the same value
    """

    return sorted(dictList, key=lambda k: k.get(key, 0.0), reverse = reverse)


def getLargestValues(dictList, key, n = 1):
    """
    _getLargestValues_

    Take a list of dictionaries, sort them by the value of a
    particular key, and return the n largest entries.

    Key must be a numerical key.
    """

    sortedList = sortDictionaryListByKey(dictList = dictList, key = key,
                                         reverse = True)

    return sortedList[:n]
