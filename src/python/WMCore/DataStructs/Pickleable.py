#!/usr/bin/env python
"""
_Pickleable_

An object that can be persisted using pickle

"""
__all__ = []


from WMCore.DataStructs.WMObject import WMObject

class Pickleable(WMObject):
    def save(self):
        """
        Pickle the object
        """
        pass

    def load(self):
        """
        Load the object from a pickle file
        """
        pass
