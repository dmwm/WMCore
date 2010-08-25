#!/usr/bin/env python
"""
Oracle implementation of File.Get
"""

__revision__ = "$Id: GetByLFN.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByLFN import GetByLFN as MySQLGetByLFN

class GetByLFN(MySQLGetByLFN):
    """
    Oracle implementation of File.Get
    """



