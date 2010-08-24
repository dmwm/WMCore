#!/usr/bin/env python
"""
_File_

Data object that contains details for a single file

"""
__all__ = []
__revision__ = "$Id: File.py,v 1.4 2008/07/21 17:23:44 metson Exp $"
__version__ = "$Revision: 1.4 $"
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
        self.dict['locations'] = Set()
        
    def setLocation(self, se):
        self.dict['locations'] = self.dict['locations'] | Set(self.makelist(se))

    def __cmp__(self, rhs):
        """
        Sort files in run number and lumi section order
        """
        if self.dict['run'] == rhs.run:
            return cmp(self.dict['lumi'], rhs.lumi)
        return cmp(self.dict['run'], rhs.run)
    
    def __eq__(self, rhs):
        """
        File is equal if it has the same name, size, runs events and lumi
        """
        eq = self.dict['lfn'] == rhs.dict['lfn'] 
        eq = eq and self.dict['size'] == rhs.dict['size']
        eq = eq and self.dict['events'] == rhs.dict['events']
        eq = eq and self.dict['run'] == rhs.dict['run']
        eq = eq and self.dict['lumi'] == rhs.dict['lumi']
        return eq
    
    def __ne__(self, rhs):
        return not self.__eq__(rhs)