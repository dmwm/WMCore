#!/usr/bin/env python
"""
Oracle implementation of File.Heritage

Make the parentage link between two file id's
"""
__all__ = []
__revision__ = "$Id: Heritage.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Heritage import Heritage as MySQLHeritage

class Heritage(MySQLHeritage):
    """
    Oracle implementation of File.Heritage
    
    Make the parentage link between two file id's
    """
