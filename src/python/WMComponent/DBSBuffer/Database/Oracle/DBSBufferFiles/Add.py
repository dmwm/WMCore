#!/usr/bin/env python
"""
Oracle implementation of AddFile
"""

#This has been modified for Oracle

__revision__ = "$Id: Add.py,v 1.2 2009/05/18 20:14:03 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Add import Add as MySQLAdd

class Add(MySQLAdd):
    """
    Oracle implementation of AddFile
    """

    #sql = """insert into dbsbuffer_file(lfn, filesize, events, cksum, dataset, status) 
    #            values (:lfn, :filesize, :events, :cksum, (select ID from dbsbuffer_dataset where Path=:dataset), :status)"""
                

