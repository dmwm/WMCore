#!/usr/bin/env python
"""
_File_

Data object that contains details for a single file

"""
__all__ = []
__revision__ = "$Id: File.py,v 1.3 2008/07/21 16:18:41 metson Exp $"
__version__ = "$Revision: 1.3 $"
from sets import Set
from WMCore.DataStructs.Pickleable import Pickleable 
class File(Pickleable):
    """
    _File_
    Data object that contains details for a single file
    """
    def __init__(self, lfn='', size=0, events=0, run=0, lumi=0, parents=Set()):
        self.dict = {}
        self.dict["lfn"] = lfn
        self.dict["size"] = size
        self.dict["events"] = events
        self.dict["run"] = run
        self.dict["lumi"] = lumi
        self.dict["parents"] = parents
        self.dict['locations'] = []
        
    def setLocation(self, se):
        self.dict['locations'] = se