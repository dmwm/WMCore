#!/usr/bin/env python
"""
_Parentage_

SQLite implementation of Fileset.Parentage

"""
__all__ = []
__revision__ = "$Id: ParentageSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

from WMCore.WMBS.MySQL.Fileset.ParentageSQL import Parentage as FilesetParentageMySQL

class Parentage(FilesetParentageMySQL, SQLiteBase):
    sql = FilesetParentageMySQL.sql