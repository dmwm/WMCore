#!/usr/bin/env python
from WMCore.DataStructs.WMObject import WMObject
class FeederImpl(WMObject):
    """
    Interface class for WMBS feeders
    
    All subclasses should implement the __call__ and persist methods, and 
    __init__ as appropriate. persist() needs to return a dictionary.
    """
    
    def __init__(self):
        pass
    
    def __call__(self, fileset):
        raise NotImplementedError, "WMBSFeeder.__call__"
    
    def persist(self):
        raise NotImplementedError, "WMBSFeeder.persist"