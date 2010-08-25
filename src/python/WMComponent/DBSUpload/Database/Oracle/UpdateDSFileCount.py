#!/usr/bin/env python
"""
_DBSBuffer.NewFile_

Update UnMigrated File Count in DBS Buffer

"""
__revision__ = "$Id: UpdateDSFileCount.py,v 1.1 2009/06/04 21:50:25 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "anzar@fnal.gov"

from WMComponent.DBSUpload.Database.MySQL.UpdateDSFileCount import UpdateDSFileCount as MySQLUpdateDSFileCount

class UpdateDSFileCount(MySQLUpdateDSFileCount):

    """
Oracle implementation to update the UnMigrated File Count in DBS Buffer

    """

#    sql = """UPDATE dbsbuffer_dataset as A
#                   inner join (
#                      select * from dbsbuffer_dataset
#                          where Path=:path
#                   ) as B on A.ID = B.ID
#                SET A.UnMigratedFiles = (select count(*) from dbsbuffer_file f where f.dataset = B.ID AND f.status = 'NOTUPLOADED')"""

    sql = """UPDATE dbsbuffer_dataset A
          SET A.UnMigratedFiles = (select count(*) FROM dbsbuffer_file f WHERE f.status = 'NOTUPLOADED' and f.dataset = A.ID AND A.Path = :path)
          """


#    sql = """UPDATE dbsbuffer_dataset A
#          SET A.UnMigratedFiles = (SELECT count(*) FROM dbsbuffer_file f WHERE f.status = 'NOTUPLOADED' AND
#          f.dataset = (SELECT ID FROM dbsbuffer_dataset WHERE Path = :path))
#          """
