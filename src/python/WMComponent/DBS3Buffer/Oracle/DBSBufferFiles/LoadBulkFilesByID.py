#!/usr/bin/env python
"""
_GetByID_

Oracle implementation of DBSBufferFiles.GetByID
"""




import logging

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles import LoadBulkFilesByID as MySQLLoadBulkFilesByID

class LoadBulkFilesByID(MySQLLoadBulkFilesByID):
    """
    Same as MySQL

    """
