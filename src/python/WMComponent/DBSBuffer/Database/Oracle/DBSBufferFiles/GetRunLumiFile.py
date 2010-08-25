#!/usr/bin/env python
"""
Oracle implementation of GetRunLumiFile
"""

__revision__ = "$Id: GetRunLumiFile.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.GetRunLumiFile import GetRunLumiFile as MySQLGetRunLumiFile


class GetRunLumiFile(MySQLGetRunLumiFile):
    """
    Oracle implementation of GetRunLumiFile
    """



