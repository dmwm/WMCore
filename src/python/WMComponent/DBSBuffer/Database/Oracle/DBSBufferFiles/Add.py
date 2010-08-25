#!/usr/bin/env python
"""
Oracle implementation of AddFile
"""

#This has been modified for Oracle

__revision__ = "$Id: Add.py,v 1.1 2009/05/15 16:47:41 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.Add import Add as MySQLAdd

class Add(MySQLAdd):

    sql = """insert into dbsbuffer_file(lfn, "size", events, cksum, dataset, status) 
                values (:lfn, :filesize, :events, :cksum, (select ID from dbsbuffer_dataset where Path=:dataset), :status)"""
                

