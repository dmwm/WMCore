#!/usr/bin/env python
"""
_WMObject_

Helper class that other objects should inherit from

"""
__all__ = []
__revision__ = "$Id: WMObject.py,v 1.4 2008/11/11 11:26:47 metson Exp $"
__version__ = "$Revision: 1.4 $"

from sets import Set

class WMObject(object):
    """
    Helper class that other objects should inherit from
    """
    def makelist(self, thelist):
        """
        Simple method to ensure thelist is a list
        """
        if isinstance(thelist, (Set, set)):
            thelist = list(thelist)
        elif not isinstance(thelist, list):
            thelist = [thelist]
        return thelist
    
    def makeset(self, theset):
        """
        Simple method to ensure theset is a set
        """
        if not isinstance(theset, Set):
            theset = Set(self.makelist(theset))
        return theset