#!/usr/bin/env python
"""
_KillWorkflow_

MySQL implementation of Subscriptions.KillWorkflow
"""

from WMCore.Database.DBFormatter import DBFormatter

class KillWorkflow(DBFormatter):
    """
    _KillWorkflow_

    Mark all files that are not complete/failed and belong to a particular
    workflow as complete so they get clean up.  Ignore Cleanup and
    LogCollect subscriptions as we still want those to run.
    """
    sql = """INSERT INTO wmbs_sub_files_complete (subscription, fileid)
               SELECT wmbs_subscription.id, wmbs_fileset_files.fileid
                      FROM wmbs_workflow
                 INNER JOIN wmbs_subscription ON
                   wmbs_workflow.id = wmbs_subscription.workflow AND
                   wmbs_subscription.subtype IN
                    (SELECT id FROM wmbs_sub_types
                     WHERE name != 'Cleanup' %s)
                 INNER JOIN wmbs_fileset_files ON
                   wmbs_subscription.fileset = wmbs_fileset_files.fileset
                 LEFT OUTER JOIN wmbs_sub_files_complete ON
                   wmbs_subscription.id = wmbs_sub_files_complete.subscription AND
                   wmbs_fileset_files.fileid = wmbs_sub_files_complete.fileid
                 LEFT OUTER JOIN wmbs_sub_files_failed ON
                   wmbs_subscription.id = wmbs_sub_files_failed.subscription AND
                   wmbs_fileset_files.fileid = wmbs_sub_files_failed.fileid
               WHERE wmbs_sub_files_complete.fileid IS Null AND
                     wmbs_sub_files_failed.fileid IS Null AND
                     wmbs_workflow.name = :workflowname"""

    delAcq = """DELETE FROM wmbs_sub_files_acquired WHERE subscription IN
                  (SELECT wmbs_subscription.id FROM wmbs_workflow
                     INNER JOIN wmbs_subscription ON
                       wmbs_workflow.id = wmbs_subscription.workflow AND
                       wmbs_workflow.name = :workflowname AND
                       wmbs_subscription.subtype IN
                         (SELECT id FROM wmbs_sub_types
                          WHERE name != 'Cleanup' %s))"""

    delAva = """DELETE FROM wmbs_sub_files_available WHERE subscription IN
                  (SELECT wmbs_subscription.id FROM wmbs_workflow
                     INNER JOIN wmbs_subscription ON
                       wmbs_workflow.id = wmbs_subscription.workflow AND
                       wmbs_workflow.name = :workflowname AND
                       wmbs_subscription.subtype IN
                         (SELECT id FROM wmbs_sub_types
                          WHERE name != 'Cleanup' %s))"""

    def execute(self, workflowName, deleteLogCollect=False, conn=None, transaction=False):
        if deleteLogCollect:
            # need a space in str
            condition = " AND name != 'LogCollect'"
        else:
            condition = ""
            
        sql = self.sql % condition
        delAcq = self.delAcq % condition
        delAva = self.delAva % condition
        
        self.dbi.processData(sql, {"workflowname": workflowName},
                             conn = conn, transaction = transaction)
        self.dbi.processData(delAcq, {"workflowname": workflowName},
                             conn = conn, transaction = transaction)
        self.dbi.processData(delAva, {"workflowname": workflowName},
                             conn = conn, transaction = transaction)
        return
