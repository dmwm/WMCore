#!/usr/bin/env python
"""
Oracle implementation of File.SetParentage

Make the parentage link between two file lfns in bulk
"""

from WMCore.WMBS.MySQL.Files.SetParentage import SetParentage as MySQLSetParentage

class SetParentage(MySQLSetParentage):
    sql = """INSERT INTO wmbs_file_parent (child, parent)
               SELECT
                 (SELECT id FROM wmbs_file_details WHERE lfn = :child),
                 (SELECT id FROM wmbs_file_details WHERE lfn = :parent) FROM DUAL
               WHERE NOT EXISTS
                 (SELECT child FROM wmbs_file_parent WHERE
                    child = (SELECT id FROM wmbs_file_details WHERE lfn = :child) AND
                    parent = (SELECT id FROM wmbs_file_details WHERE lfn = :parent))"""
