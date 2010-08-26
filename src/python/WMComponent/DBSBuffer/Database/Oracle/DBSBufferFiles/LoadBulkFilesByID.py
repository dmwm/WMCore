#!/usr/bin/env python
"""
_GetByID_

Oracle implementation of DBSBufferFiles.GetByID
"""

__revision__ = "$Id: LoadBulkFilesByID.py,v 1.1 2010/05/14 18:53:59 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles import LoadBulkFilesByID as MySQLLoadBulkFilesByID

class LoadBulkFilesByID(MySQLLoadBulkFilesByID):
    """
    Same as MySQL

    """
