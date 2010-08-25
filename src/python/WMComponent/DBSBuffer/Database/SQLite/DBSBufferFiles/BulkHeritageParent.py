#!/usr/bin/env python
"""
_HeritageLFNParent_

SQLite implementation of DBSBufferFiles.HeritageLFNParent
"""




from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.BulkHeritageParent import BulkHeritageParent as MySQLBulkHeritageParent


class BulkHeritageParent(DBFormatter):
    """
    Commit parentage information in bulk


    """
