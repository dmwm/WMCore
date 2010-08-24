#!/usr/bin/env python
"""
_Subscription_

workflow + fileset = subscription

TODO: Add some kind of tracking for state of files
"""
__all__ = []
__revision__ = "$Id: Subscription.py,v 1.1 2008/07/03 17:00:21 metson Exp $"
__version__ = "$Revision: 1.1 $"
from WMCore.DataStructs.Pickleable import Pickleable

class Subscription(Pickleable):
    def __init__(self, fileset = None, workflow = None, 
               split_algo = 'File', type = "Processing"):
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.split_algo = split_algo
        
    def getWorkflow(self):
        return self.workflow
    
    def getFileset(self):
        return self.fileset