#!/usr/bin/env python
"""
_HeritageLFNParent_

Oracle implementation of DBSBufferFiles.HeritageLFNParent
"""




from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.HeritageLFNParent import \
     HeritageLFNParent as MySQLHeritageLFNParent

class HeritageLFNParent(MySQLHeritageLFNParent):

    sql = """INSERT INTO dbsbuffer_file_parent (child, parent)
               VALUES (
                 (SELECT dfc.id FROM dbsbuffer_file dfc WHERE dfc.lfn = :child),
                 (SELECT dfp.id FROM dbsbuffer_file dfp WHERE dfp.lfn = :lfn)
               )"""
