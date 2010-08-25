#!/usr/bin/env python
"""
_Delete_

Oracle implementation of Delete
"""

__revision__ = "$Id: Delete.py,v 1.2 2009/07/14 19:13:57 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    _Delete_
    
    Oracle implementation of Delete
    """
    pass
