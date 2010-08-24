#!/usr/bin/env python
"""
SQLite implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""
__all__ = []



from WMCore.WMBS.MySQL.Files.SetParentage import SetParentage as MySQLSetParentage

class SetParentage(MySQLSetParentage):
    """
    _SetParentage_


    Identical to MySQL version
    """
    sql = MySQLSetParentage.sql.replace('INSERT IGNORE', 'INSERT OR IGNORE')