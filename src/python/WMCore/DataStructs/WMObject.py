#!/usr/bin/env python
"""
_WMObject_

Helper class that other objects should inherit from

"""
__all__ = []
__revision__ = "$Id: WMObject.py,v 1.9 2009/12/15 14:13:51 spiga Exp $"
__version__ = "$Revision: 1.9 $"


class WMObject(object):
    """
    Helper class that other objects should inherit from
    """
    def __init__(self, config = {}):
        #Config is a WMCore.Configuration
        self.config = config

    def makelist(self, thelist):
        """
        Simple method to ensure thelist is a list
        """
        if isinstance(thelist, (set, set)):
            thelist = list(thelist)
        elif not isinstance(thelist, list):
            thelist = [thelist]
        return thelist

    def makeset(self, theset):
        """
        Simple method to ensure theset is a set
        """
        if not isinstance(theset, set):
            theset = set(self.makelist(theset))
        return theset

    def flatten(self, list):
        """
        If a list has only one element return just that element, otherwise
        return the original list
        """
        if len(list) == 1:
            return list[0]
        return list
