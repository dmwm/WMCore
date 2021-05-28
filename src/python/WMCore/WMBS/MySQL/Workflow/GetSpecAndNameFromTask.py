#!/usr/bin/env python
"""
_GetSpecAndNameFromTask_

MySQL implementation of Workflow.GetSpecAndNameFromTask

Created on Oct 9, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetSpecAndNameFromTask(DBFormatter):
    """
    _GetSpecAndNameFromTask_

    Gets the spec path and workflow name from a task
    """

    sql = """SELECT name, spec, task FROM wmbs_workflow WHERE task = :task"""

    def execute(self, tasks, conn = None, transaction = False):
        """
        _execute_

        Runs the query
        """

        if not isinstance(tasks, list):
            tasks = [tasks]

        binds = []
        for task in tasks:
            binds.append({'task' : task})

        if not binds:
            return {}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        unsortedResult = self.formatDict(result)

        sortedResult = {}
        for entry in unsortedResult:
            sortedResult[entry['task']] = {'spec' : entry['spec'], 'name' : entry['name']}

        return sortedResult
