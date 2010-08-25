#!/usr/bin/env python
"""
Oracle implementation of File.Get
"""

__revision__ = "$Id: GetByID.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByID import GetByID as MySQLGetByID

class GetByID(MySQLGetByID):
    """
    Oracle implementation of File.Get
    """
