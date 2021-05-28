#!/usr/bin/env python
"""
_RemoveDuplicates_

MySQL implementation of Workflow.RemoveDuplicates
"""

from future.utils import listvalues

from WMCore.Database.DBFormatter import DBFormatter

class RemoveDuplicates(DBFormatter):
    sql = """SELECT lfn FROM wmbs_file_details
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_details.id = wmbs_fileset_files.fileid
               INNER JOIN wmbs_subscription ON
                 wmbs_fileset_files.fileset = wmbs_subscription.fileset
               INNER JOIN wmbs_workflow ON
                 wmbs_subscription.workflow = wmbs_workflow.id
             WHERE wmbs_file_details.lfn = :lfn AND
                   wmbs_workflow.name = :workflow"""

    def execute(self, files, workflow, conn = None, transaction = False):
        binds = []
        newFiles = {}
        for file in files:
            binds.append({"lfn": file["lfn"], "workflow": workflow})
            newFiles[file["lfn"]] = file

        results = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        for result in self.formatDict(results):
            if result["lfn"] in list(newFiles):
                del newFiles[result["lfn"]]

        return listvalues(newFiles)
