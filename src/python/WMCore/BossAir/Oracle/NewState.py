#!/usr/bin/env python
"""
_NewState_

Oracle implementation for labeling a job Complete
"""


from WMCore.BossAir.MySQL.NewState import NewState as MySQLNewState

class NewState(MySQLNewState):
    """
    _NewState_

    Insert new states into bl_status
    """


    sql = """INSERT INTO bl_status (id, name) SELECT bl_status_SEQ.nextval, :name FROM dual
               WHERE NOT EXISTS (SELECT stat.id FROM bl_status stat WHERE stat.name = :name)"""
