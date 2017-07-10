#!/usr/bin/env python
"""
_ListSubsAndFilesetsFromWorkflow_

Creates a mapping between subscriptions id, fileset names and workflow
tasks.
"""
from __future__ import division

from WMCore.Database.DBFormatter import DBFormatter


class ListSubsAndFilesetsFromWorkflow(DBFormatter):
    """ Returns a list of tuples """
    sql = """SELECT wmbs_subscription.id as subID, wmbs_fileset.name AS fileset,
                 wmbs_workflow.task AS wfTask, wmbs_subscription.split_algo,
                 wmbs_sub_types.name AS subType
               FROM wmbs_subscription
                 INNER JOIN wmbs_fileset ON wmbs_subscription.fileset = wmbs_fileset.id
                 INNER JOIN wmbs_workflow ON wmbs_subscription.workflow = wmbs_workflow.id
                 INNER JOIN wmbs_sub_types ON wmbs_subscription.subtype = wmbs_sub_types.id
               WHERE wmbs_workflow.name = :workflow
               ORDER BY wmbs_fileset.name, wmbs_workflow.task"""

    def format(self, results):
        "Build a list of tuples"
        result = []
        results = DBFormatter.format(self, results)
        for item in results:
            result.append(tuple(item))

        return result

    def execute(self, workflow, returnTuple=False, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, {"workflow": workflow},
                                      conn=conn, transaction=transaction)
        if returnTuple:
            # easier for comparison
            return self.format(result)
        else:
            return self.formatDict(result)
