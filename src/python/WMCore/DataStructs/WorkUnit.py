#!/usr/bin/env python
"""
_WorkUnit_

Data object that contains details for a single work unit

"""

from __future__ import absolute_import, division, print_function

import sys
import time
from functools import total_ordering

from WMCore.DataStructs.WMObject import WMObject


@total_ordering
class WorkUnit(WMObject, dict):
    """
    _WorkUnit_
    Data object that contains details for a single work unit
    corresponding to tables workunit and frl_workunit_assoc
    """

    fieldsToCopy = ['taskid', 'retry_count', 'last_unit_count', 'last_submit_time', 'status', 'firstevent',
                    'lastevent', 'fileid']
    fieldsForInfo = fieldsToCopy + ['run_lumi']

    def __init__(self, taskID=None, retryCount=0, lastUnitCount=None, lastSubmitTime=int(time.time()),
                 status=0, firstEvent=1, lastEvent=sys.maxsize, fileid=None, runLumi=None):
        super(WorkUnit, self).__init__(self)
        self.setdefault('taskid', taskID)
        self.setdefault('retry_count', retryCount)
        self.setdefault('last_unit_count', lastUnitCount)
        self.setdefault('last_submit_time', lastSubmitTime)
        self.setdefault('status', status)

        self.setdefault('firstevent', firstEvent)
        self.setdefault('lastevent', lastEvent)
        self.setdefault('fileid', fileid)
        self.setdefault('run_lumi', runLumi)

    def __lt__(self, rhs):
        """
        Compare work units in task id, run, lumi, first event, last event
        """

        if self['taskid'] != rhs['taskid']:
            return self['taskid'] < rhs['taskid']
        if self['run_lumi'].run != rhs['run_lumi'].run:
            return self['run_lumi'].run < rhs['run_lumi'].run
        if self['run_lumi'].lumis != rhs['run_lumi'].lumis:
            return self['run_lumi'].lumis < rhs['run_lumi'].lumis
        if self['first_event'] != rhs['first_event']:
            return self['first_event'] < rhs['first_event']
        return self['last_event'] < rhs['last_event']

    def __eq__(self, rhs):
        """
        Work unit is equal if it has the same task, run, and lumi
        """

        return (self['taskid'] == rhs['taskid'] and self['run_lumi'].run == self['run_lumi'].run and
                self['run_lumi'].lumis == self['run_lumi'].lumis and self['firstevent'] == rhs['firstevent'] and
                self['lastevent'] == rhs['lastevent'])

    def __hash__(self):
        """
        Hash function for this dict.
        """

        return hash(frozenset(self.items()))

    def json(self, thunker=None):
        """
        _json_

        Serialize the object.  Only copy select fields and construct one new field.
        """

        jsonDict = {k: self[k] for k in WorkUnit.fieldsToCopy}
        jsonDict["run_lumi"] = {"run_number": self['run_lumi'].run, "lumis": self['run_lumi'].lumis}

        return jsonDict

    def __to_json__(self, thunker=None):
        """
        __to_json__

        This is the standard way we jsonize other objects.
        Included here so we have a uniform method.
        """
        return self.json(thunker)

    def getInfo(self):
        """
        Returns: tuple of parameters for the work unit
        """
        return tuple(self[x] for x in WorkUnit.fieldsForInfo)
