#!/usr/bin/env python
"""
_Workflow_

A class that describes some work to be undertaken on some files
"""

__all__ = []
__revision__ = "$Id: Workflow.py,v 1.3 2009/04/01 18:45:22 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.DataStructs.Pickleable import Pickleable

class Workflow(Pickleable):
    def __init__(self, spec=None, owner=None, name='workflow'):        
        #TODO: define a url-like scheme for spec's and enforce it here
        self.spec = spec
        self.name = name
        self.owner = owner
        self.outputMap = {}

    def addOutput(self, outputIdentifier, outputFileset):
        """
        _addOutput_

        Associate an output of this workflow with a particular fileset.
        """
        self.outputMap[outputIdentifier] = outputFileset
        return
