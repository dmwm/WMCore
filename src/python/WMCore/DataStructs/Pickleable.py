#!/usr/bin/env python
"""
_Pickleable_

An object that can be persisted using pickle

"""
__all__ = []
__revision__ = "$Id: Pickleable.py,v 1.1 2008/07/03 17:00:40 metson Exp $"
__version__ = "$Revision: 1.1 $"
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