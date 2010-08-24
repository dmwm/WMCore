#!/usr/bin/env python

class FeederImpl:
    """
    Interface class for WMBS feeders
    """
    
    
    def __init__(self):
        self.connectionAttempts = 5
    
    
    def __call__(self, fileset):
        raise NotImplementedError, "WMBSFeeder.__call__"