#!/usr/bin/env python
"""
_Mask_

Basic Mask object that can be used to restrict the run/lumi/event range in a
job in two ways:

- Inclusive: The stuff within the mask is processed
- Exclusive: The stuff outside of the mask is processed

"""
__author__ = "evansde@fnal.gov"
__revision__ = "$Id: Mask.py,v 1.3 2009/06/05 16:36:22 mnorman Exp $"
__version__ = "$Revision: 1.3 $"




class Mask(dict):
    """
    _Mask_



    """
    def __init__(self):
        dict.__init__(self)
        self.inclusive = True
        self.setdefault("FirstEvent", None)
        self.setdefault("LastEvent", None)
        self.setdefault("FirstLumi", None)
        self.setdefault("LastLumi", None)
        self.setdefault("FirstRun", None)
        self.setdefault("LastRun", None)


    def setMaxAndSkipEvents(self, maxEvents, skipEvents):
        """
        _setMaxAndSkipEvents_

        Set FirstEvent & LastEvent fields as max & skip events

        """
        self['FirstEvent'] = skipEvents
        self['LastEvent']  = skipEvents + maxEvents
        return

    def setMaxAndSkipLumis(self, maxLumis, skipLumi):
        """
        _setMaxAndSkipLumis

        Set the Maximum number of lumi sections and the starting point

        """

        self['FirstLumi'] = skipLumi
        self['LastLumi']  = skipLumi + maxLumis

        return


    def setMaxAndSkipRuns(self, maxRuns, skipRun):
        """
        _setMaxAndSkipRuns

        Set the Maximum number of runss and the starting point

        """

        self['FirstRun'] = skipRun
        self['LastRun']  = skipRun + maxRuns

        return

    def getMaxEvents(self):
        """
        _getMaxEvents_

        return maxevents setting

        """
        if (self['LastEvent'] == None) or (self['FirstEvent'] == None):
            return None
        return self['LastEvent'] - self['FirstEvent']


    def getMax(self, type = None):
        """
        _getMax_

        returns the maximum number of runs/events/etc of the type of the type string

        """

        if not self.has_key('First%s' %(type)):
            return None
        if (self['First%s'%(type)] == None) or (self['Last%s'%(type)] == None):
            return None
        return self['Last%s'%(type)] - self['First%s'%(type)]




class InclusiveMask(Mask):
    """
    _InclusiveMask_

    Mask with Inclusive flag set

    """
    def __init__(self):
        Mask.__init__(self)
        self.inclusive = True

class ExclusiveMask(Mask):
    """
    _ExclusiveMask_

    Mask with Inclusive flag set to false

    """
    def __init__(self):
        Mask.__init__(self)
        self.inclusive = False


