#!/usr/bin/env python
"""
_KillWorkflow_

Oracle implementation of Subscriptions.KillWorkflow
"""

from WMCore.WMBS.MySQL.Subscriptions.KillWorkflow import KillWorkflow as MySQLKillWorkflow

class KillWorkflow(MySQLKillWorkflow):
    """
    _KillWorkflow_

    Mark all files that are not complete/failed and belong to a particular
    workflow as failed.  Ignore Cleanup and LogCollect subscriptions as we
    still want those to run.
    """
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, fileid)
               SELECT wmbs_subscription.id, wmbs_fileset_files.fileid
                      FROM wmbs_workflow
                 INNER JOIN wmbs_subscription ON
                   wmbs_workflow.id = wmbs_subscription.workflow AND
                   wmbs_subscription.subtype IN
                    (SELECT id FROM wmbs_sub_types
                     WHERE name != 'Cleanup' AND name != 'LogCollect')
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
