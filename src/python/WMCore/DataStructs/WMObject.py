#!/usr/bin/env python
"""
_WMObject_

Helper class that other objects should inherit from

"""
__all__ = []
__revision__ = "$Id: WMObject.py,v 1.3 2008/11/03 10:13:15 jacksonj Exp $"
__version__ = "$Revision: 1.3 $"

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
        Simple method to ensure thelist is a set
        """
        if not isinstance(theset, Set):
            theset = Set(self.makelist(theset))
        return theset