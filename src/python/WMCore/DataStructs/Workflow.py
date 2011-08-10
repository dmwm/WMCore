#!/usr/bin/env python
"""
_Workflow_

A class that describes some work to be undertaken on some files
"""

from WMCore.DataStructs.Pickleable import Pickleable

class Workflow(Pickleable):
    def __init__(self, spec = None, owner = None, dn = None , name = None,
                 task = None, wfType = None):
        self.spec = spec
        self.name = name
        # person making the request
        self.owner = owner
        self.dn = dn
        # task is the name of the task within the Workload
        self.task = task
        self.wfType = wfType
        self.outputMap = {}

    def addOutput(self, outputIdentifier, outputFileset,
                  mergedOutputFileset = None):
        """
        _addOutput_

        Associate an output of this workflow with a particular fileset.
        """
        mappingDict = {"output_fileset": outputFileset,
                       "merged_output_fileset": mergedOutputFileset}

        if self.outputMap.has_key(outputIdentifier):
            self.outputMap[outputIdentifier].append(mappingDict)
        else:
            self.outputMap[outputIdentifier] = [mappingDict]

        return
