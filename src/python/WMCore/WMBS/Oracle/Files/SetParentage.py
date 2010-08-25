#!/usr/bin/env python
"""
Oracle implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""
__all__ = []



from WMCore.WMBS.MySQL.Files.SetParentage import SetParentage as MySQLSetParentage

class SetParentage(MySQLSetParentage):
    """
    _SetParentage_


    Identical to MySQL version
    """

    sql = """INSERT IGNORE INTO wmbs_file_parent (child, parent)
             SELECT DISTINCT wfd1.id, wfd2.id
             FROM wmbs_file_details wfd1 INNER JOIN wmbs_file_details wfd2
             WHERE wfd1.lfn = :child
             AND wfd2.lfn = :parent
             AND NOT EXISTS (SELECT child FROM wmbs_file_parent WHERE child = wfd1.id AND parent = wfd2.id)
    """
