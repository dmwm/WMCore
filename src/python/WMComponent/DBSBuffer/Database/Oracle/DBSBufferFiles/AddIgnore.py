#!/usr/bin/env python
"""
_AddIgnore_

MySQL implementation of DBSBufferFiles.AddIgnore
"""

__revision__ = "$Id: AddIgnore.py,v 1.1 2009/11/02 20:12:53 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddIgnore import AddIgnore as \
     MySQLAddIgnore

class AddIgnore(MySQLAddIgnore):
    sql = """INSERT INTO dbsbuffer_file (lfn, dataset_algo, status) 
                SELECT :lfn, :dataset_algo, :status FROM DUAL WHERE NOT EXISTS
                  (SELECT id FROM dbsbuffer_file WHERE lfn = :lfn)"""
