#!/usr/bin/env python
"""
_WMObject_

Helper class that other objects should inherit from

"""
__all__ = []
__revision__ = "$Id: WMObject.py,v 1.1 2008/07/03 16:59:46 metson Exp $"
__version__ = "$Revision: 1.1 $"
class WMObject(object):
    """
    Helper class that other objects should inherit from
    """
    def makelist(self, thelist):
        """
        Simple method to ensure thelist is a list
        """
        if not isinstance(thelist, list):
            thelist = [thelist]
        return thelist