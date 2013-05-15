#!/usr/bin/env python
"""
_InsertWorkflow_

Oracle implementation of DBSBuffer.InsertWorkflow
"""

from WMComponent.DBS3Buffer.MySQL.InsertWorkflow import InsertWorkflow as MySQLInsertWorkflow

class InsertWorkflow(MySQLInsertWorkflow):
    """
    _InsertWorkflow_

    Insert a workflow using the name and task
    """

    sql = """INSERT INTO dbsbuffer_workflow
                (id, name, task,
                block_close_max_wait_time,
                block_close_max_files,
                block_close_max_events,
                block_close_max_size)
             SELECT dbsbuffer_workflow_seq.nextval,
                    :name,
                    :task,
                    :blockMaxCloseTime,
                    :blockMaxFiles,
                    :blockMaxEvents,
                    :blockMaxSize
             FROM DUAL
             WHERE NOT EXISTS (
               SELECT id
               FROM dbsbuffer_workflow
               WHERE name = :name
               AND task = :task
             )"""
