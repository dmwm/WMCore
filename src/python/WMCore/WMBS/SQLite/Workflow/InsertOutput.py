#!/usr/bin/env python
"""
_InsertOutput_

SQLite implementation of Workflow.InsertOutput
"""

from WMCore.WMBS.MySQL.Workflow.InsertOutput import InsertOutput as InsertOutputMySQL

class InsertOutput(InsertOutputMySQL):
    sql = """INSERT INTO wmbs_workflow_output (workflow_id, output_identifier,
                                               output_fileset)
               SELECT :workflow AS workflow_id, :output AS output_identifier,
                 :fileset AS output_fileset WHERE NOT EXISTS
               (SELECT workflow_id FROM wmbs_workflow_output
                 WHERE :workflow = workflow_id AND :output = output_identifier)"""
