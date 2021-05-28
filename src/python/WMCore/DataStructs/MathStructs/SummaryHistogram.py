"""
_SummaryHistogram_

Histogram module, to be used by the TaskArchiver
to store histograms in the summary.

Created on Nov 16, 2012

@author: dballest
"""
from builtins import str
from WMCore.DataStructs.WMObject import WMObject

class SummaryHistogram(WMObject):
    """
    _SummaryHistogram_

    Histogram object, provides familiar CRUD methods
    which take care of most of the statistical
    calculations when adding points, this object
    can also be converted into a dictionary
    for JSON documents. It knows how to combine
    with other histograms and create itself from
    a dictionary provided it has matching structure.

    This is an interface, the real work is done
    by the ContinuousSummaryHistogram and
    DiscreteSummaryHistogram objects
    """

    def __init__(self, title = None, xLabel = None):
        """
        __init__
        Initialize the elements in the object.
        """

        # Meta-information about the histogram, it can be changed at any point
        self.title      = title
        self.xLabel     = xLabel

        # These shouldn't be touched from anything outside the SummaryHistogram object and children classes
        self.continuous         = None
        self.jsonInternal       = None
        self.data               = {}
        self.average            = None
        self.stdDev             = None

        return

    def setTitle(self, newTitle):
        """
        _setTitle_

        Set the title
        """
        self.title = newTitle
        return

    def setHorizontalLabel(self, xLabel):
        """
        _setHorizontalLabel_

        Set the label on the x axis
        """
        self.xLabel = xLabel
        return

    def addPoint(self, xValue, yLabel):
        """
        _addPoint_

        Add a point to the histogram data, a histogram
        can have many types of y values for the same x if
        x is continuous otherwise it is only one yLabel.
        They should be in a similar scale for best results.
        """
        raise NotImplementedError("SummaryHistogram objects can't be used, use either the continuous or discrete implementation")

    def toJSON(self):
        """
        _toJSON_

        Return a dictionary which is compatible
        with a JSON object
        """
        if self.continuous is None:
            raise TypeError("toJSON can't be called on a bare SummaryHistogram object")

        # Get what the children classes did
        jsonDict = {}
        jsonDict['internalData'] = self.jsonInternal or {}

        # Add the common things
        jsonDict['title']      = self.title
        jsonDict['xLabel']     = self.xLabel
        jsonDict['continuous'] = self.continuous
        jsonDict['data']       = self.data
        jsonDict['stdDev']     = self.stdDev
        jsonDict['average']    = self.average

        return jsonDict

    def __add__(self, other):
        """
        __add__

        Add two histograms, combine statistics.
        """
        raise NotImplementedError("SummaryHistogram objects can't be used, use either the continuous or discrete implementation")

    def __str__(self):
        """
        __str__

        Return the str object of the JSON
        """
        return str(self.toJSON())
