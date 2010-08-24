"""
_FilesetParentage_
MySQL implementation of FilesetParentage 
"""

__revision__ = "$Id: FilesetParentageSQL.py,v 1.2 2008/06/02 23:58:52 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase
from WMCore.WMBS.MySQL.FilesetParentageSQL import FilesetParentage as FilesetParentageMySQL

class FilesetParentage(FilesetParentageMySQL, SQLiteBase):
    sql = FilesetParentageMySQL.sql