#!/usr/bin/env python
"""
_Parentage_

SQLite implementation of Fileset.Parentage

"""
__all__ = []
__revision__ = "$Id: Parentage.py,v 1.1 2008/10/08 14:30:11 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

from WMCore.WMBS.MySQL.Fileset.Parentage import Parentage as FilesetParentageMySQL

class Parentage(FilesetParentageMySQL, SQLiteBase):
    sql = FilesetParentageMySQL.sql