#!/usr/bin/env python
"""
_Workflow_

A class that describes some work to be undertaken on some files
"""

from WMCore.DataStructs.Pickleable import Pickleable

class Workflow(Pickleable):
    def __init__(self, spec = None, owner = "unknown", dn = "unknown",
                 group = "unknown", owner_vogroup = "unknown",
                 owner_vorole = "unknown", name = None, task = None,
                 wfType = None, priority = None):
        self.spec = spec
        self.name = name
        # person making the request
        self.owner = owner
        self.dn = dn
        self.vogroup = owner_vogroup
        self.vorole = owner_vorole
        self.group = group
        # task is the name of the task within the Workload
        self.task = task
        self.wfType = wfType
        self.outputMap = {}
        self.priority = priority or 0

    def addOutput(self, outputIdentifier, outputFileset,
                  mergedOutputFileset = None):
        """
        _addOutput_

        Associate an output of this workflow with a particular fileset.
        """
        mappingDict = {"output_fileset": outputFileset,
                       "merged_output_fileset": mergedOutputFileset}

        if outputIdentifier in self.outputMap:
            self.outputMap[outputIdentifier].append(mappingDict)
        else:
            self.outputMap[outputIdentifier] = [mappingDict]

        return

    def __str__(self):
        """
        __str__

        Print out some useful info just because
        this does not inherit from dict.
        """

        d = {'id': self.id, 'spec': self.spec, 'name': self.name,
             'owner': self.owner, 'task': self.task,
             'vogroup': self.vogroup, 'vorole': self.vorole,
             'group': self.group, 'wfType': self.wfType}

        return str(d)
