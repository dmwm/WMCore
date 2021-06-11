#!/usr/bin/env python
"""
_WorkUnit_

Represents a work unit in WMBS
"""

from __future__ import absolute_import, division, print_function

import time

from WMCore.DataStructs.WorkUnit import WorkUnit as DSWorkUnit
from WMCore.WMBS.WMBSBase import WMBSBase

MAX_EVENT = 2 ** 31 - 1


class WorkUnit(WMBSBase, DSWorkUnit):
    """
    A simple object representing a file in WMBS
    """

    def __init__(self, wuid=None, taskID=None, retryCount=0, lastUnitCount=None, lastSubmitTime=int(time.time()),
                 status=0, firstEvent=1, lastEvent=MAX_EVENT, fileid=None, runLumi=None):
        WMBSBase.__init__(self)
        DSWorkUnit.__init__(self, taskID=taskID, retryCount=retryCount, lastUnitCount=lastUnitCount,
                            lastSubmitTime=lastSubmitTime, status=status, firstEvent=firstEvent, lastEvent=lastEvent,
                            fileid=fileid, runLumi=runLumi)
        self.setdefault('id', wuid)

    def exists(self):
        """
        If id is given, check with id or check with taskid, fileid, run/lumi
        """

        if self['id'] and self['id'] > 0:
            action = self.daofactory(classname='WorkUnit.ExistsByID')
            result = action.execute(wuid=self['id'], conn=self.getDBConn(), transaction=self.existingTransaction())
        elif self['taskid'] and self['fileid'] and self['run_lumi']:
            action = self.daofactory(classname='WorkUnit.ExistsByTaskFileLumi')
            result = action.execute(taskid=self['taskid'], fileid=self['fileid'], run_lumi=self['run_lumi'],
                                    conn=self.getDBConn(), transaction=self.existingTransaction())
        else:
            raise NotImplementedError("No way to look up existence without ID or task, file, run, and lumi")

        return result

    def getInfo(self):
        """
        Return the WorkUnit attributes as a tuple based off of DataStructs
        """
        dsInfo = super(WorkUnit, self).getInfo()
        return (self['id'],) + dsInfo

    def load(self):
        """
        _load_

        Load any meta data that is associated with a WorkUnit.
        """

        if self['id'] and self['id'] > 0:
            action = self.daofactory(classname="WorkUnit.GetByID")
            result = action.execute(self['id'], conn=self.getDBConn(), transaction=self.existingTransaction())
        elif self['taskid'] and self['fileid'] and self['run_lumi']:
            action = self.daofactory(classname='WorkUnit.GetByTaskFileLumi')
            result = action.execute(taskid=self['taskid'], fileid=self['fileid'],
                                    run_lumi=self['run_lumi'],
                                    conn=self.getDBConn(), transaction=self.existingTransaction())
        else:
            raise RuntimeError("Only methods to get a work unit is by ID or by task, fileid, run, lumi")

        self.update(result)
        return

    def create(self):
        """
        _create_

        Create a work unit.  If no transaction is passed in this will wrap all
        statements in a single transaction.
        """

        # TODO: Allow creation of a work unit without passing in ID

        existingTransaction = self.beginTransaction()

        if self.exists() is not False:
            self.commitTransaction(existingTransaction)
            self.load()
            # assume if the file already exist, parentage is already set.
            # or not exist yet
            return

        addAction = self.daofactory(classname="WorkUnit.Add")

        addAction.execute(taskid=self['taskid'], retry_count=self['retry_count'],
                          last_unit_count=self['last_unit_count'], last_submit_time=self['last_submit_time'],
                          status=self['status'],
                          first_event=self['firstevent'], last_event=self['lastevent'],
                          fileid=self['fileid'], run=self['run_lumi'].run, lumi=self['run_lumi'].lumis[0],
                          conn=self.getDBConn(), transaction=self.existingTransaction())

        self.commitTransaction(existingTransaction)

        return

    def delete(self):
        """
        Remove a WorkUnit from WMBS
        """
        action = self.daofactory(classname="WorkUnit.Delete")
        action.execute(wuid=self["id"], conn=self.getDBConn(), transaction=self.existingTransaction())
        return
