#!/usr/bin/env python
"""
_SetBlockStatus_

Create a new block in dbsbuffer_block.
"""




import threading
import exceptions

from WMComponent.DBSUpload.Database.MySQL.SetBlockStatus import SetBlockStatus as MySQLSetBlockStatus


class SetBlockStatus(MySQLSetBlockStatus):
    """
    _SetBlockStatus_

    """
    sql = """INSERT INTO dbsbuffer_block (blockname, location)
               SELECT :block, (SELECT id FROM dbsbuffer_location WHERE se_name = :location)
               WHERE NOT EXISTS (SELECT blockname FROM dbsbuffer_block WHERE blockname = :block
               and location = (SELECT id FROM dbsbuffer_location WHERE se_name = :location))
    """
