#!/usr/bin/env python
"""
_DeleteFile_

Oracle implementation of DeleteFile

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    _DeleteFile_
    
    Oracle implementation of DeleteFile
    
    """
