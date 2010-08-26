#!/usr/bin/env python
"""
_InsertOutput_

SQLite implementation of Workflow.InsertOutput
"""

__all__ = []
__revision__ = "$Id: InsertOutput.py,v 1.3 2009/12/04 21:27:54 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Workflow.InsertOutput import InsertOutput as InsertOutputMySQL

class InsertOutput(InsertOutputMySQL):
    sql = """INSERT INTO wmbs_workflow_output (workflow_id, output_identifier,
                                               output_fileset, output_parent)
               SELECT :workflow AS workflow_id, :output AS output_identifier,
                 :fileset AS output_fileset, :parent AS output_parent
                 WHERE NOT EXISTS
               (SELECT workflow_id FROM wmbs_workflow_output
                 WHERE :workflow = workflow_id AND :output = output_identifier)
          """
