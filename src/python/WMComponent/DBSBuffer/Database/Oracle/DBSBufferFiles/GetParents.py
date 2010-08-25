#!/usr/bin/env python
"""
Oracle implementation of File.GetParents

Return a list of lfn's which are parents for a file.
"""

__revision__ = "$Id: GetParents.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetParents import GetParents as MySQLGetParents

class GetParents(MySQLGetParents):
    """
    Oracle implementation of File.GetParents
    
    Return a list of lfn's which are parents for a file.
    """
