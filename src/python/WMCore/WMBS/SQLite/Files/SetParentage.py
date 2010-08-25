#!/usr/bin/env python
"""
SQLite implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""
__all__ = []
__revision__ = "$Id: SetParentage.py,v 1.2 2010/08/17 14:50:12 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.SetParentage import SetParentage as MySQLSetParentage

class SetParentage(MySQLSetParentage):
    """
    _SetParentage_


    Identical to MySQL version
    """
    sql = MySQLSetParentage.sql.replace('INSERT IGNORE', 'INSERT OR IGNORE')