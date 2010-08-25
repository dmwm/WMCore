#!/usr/bin/env python
"""
_Workflow_

A class that describes some work to be undertaken on some files
"""

__all__ = []



from WMCore.DataStructs.Pickleable import Pickleable

class Workflow(Pickleable):
    def __init__(self, spec=None, owner=None, name='workflow', task=None):
        #TODO: define a url-like scheme for spec's and enforce it here
        # spec is a URL to the WMWorkload file
        self.spec = spec
        # name is the name of this workflow instance
        self.name = name #NEEDED? Redundant...
        # person making the request
        self.owner = owner
        # task is the name of the task within the Workload
        self.task = task
        self.outputMap = {}

    def addOutput(self, outputIdentifier, outputFileset):
        """
        _addOutput_

        Associate an output of this workflow with a particular fileset.
        """
        self.outputMap[outputIdentifier] = outputFileset
        return
