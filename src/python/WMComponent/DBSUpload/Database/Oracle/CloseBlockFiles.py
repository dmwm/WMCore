#!/usr/bin/env python
"""
_CloseBlockFiles_

Oracle implementation of CloseBlockFiles
"""




from WMComponent.DBSUpload.Database.MySQL.CloseBlockFiles import CloseBlockFiles as MySQLCloseBlockFiles

class CloseBlockFiles(MySQLCloseBlockFiles):
    """
    Oracle version

    """
