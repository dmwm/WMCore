#!/usr/bin/env python
"""
_Workflow_

A class that describes some work to be undertaken on some files
"""

__all__ = []
__revision__ = "$Id: Workflow.py,v 1.4 2009/05/08 16:39:16 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.DataStructs.Pickleable import Pickleable

class Workflow(Pickleable):
    def __init__(self, spec=None, owner=None, name='workflow', task=None):        
        #TODO: define a url-like scheme for spec's and enforce it here
        self.spec = spec
        self.name = name
        self.owner = owner
        self.task = task
        self.outputMap = {}

    def addOutput(self, outputIdentifier, outputFileset):
        """
        _addOutput_

        Associate an output of this workflow with a particular fileset.
        """
        self.outputMap[outputIdentifier] = outputFileset
        return
