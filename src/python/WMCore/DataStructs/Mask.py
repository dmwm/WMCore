#!/usr/bin/env python
"""
_Mask_

Basic Mask object that can be used to restrict the run/lumi/event range in a
job in two ways:

- Inclusive: The stuff within the mask is processed
- Exclusive: The stuff outside of the mask is processed

"""
__author__ = "evansde@fnal.gov"
__revision__ = "$Id: Mask.py,v 1.2 2008/09/20 12:50:16 evansde Exp $"
__version__ = "$Revision: 1.2 $"




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

    def getMaxEvents(self):
        """
        _getMaxEvents_

        return maxevents setting

        """
        if (self['LastEvent'] == None) or (self['FirstEvent'] == None):
            return None
        return self['LastEvent'] - self['FirstEvent']




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


