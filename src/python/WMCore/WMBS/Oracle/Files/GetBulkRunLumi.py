#!/usr/bin/env python
"""
_GetBulkRunLumi_

Oracle implementation of Files.GetBulkRunLumi
"""

from WMCore.WMBS.MySQL.Files.GetBulkRunLumi import GetBulkRunLumi as MySQLGetBulkRunLumi

class GetBulkRunLumi(MySQLGetBulkRunLumi):
    sql = """SELECT flr.run AS run, flr.lumi AS lumi, flr.fileid AS id
               FROM wmbs_file_runlumi_map flr
               WHERE flr.fileid = :id"""
