#!/usr/bin/env python
"""
Oracle implementation of File.Get
"""

__revision__ = "$Id: GetByLFN.py,v 1.2 2009/05/18 20:14:03 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetByLFN import GetByLFN as MySQLGetByLFN

class GetByLFN(MySQLGetByLFN):
    """
    Oracle implementation of File.Get
    """


    sql = """select files.id, files.lfn, files.filesize, files.events, files.cksum, ds.Path, files.status
    from dbsbuffer_file files
    join dbsbuffer_dataset ds
         on files.dataset=ds.ID
         where lfn = :lfn"""



