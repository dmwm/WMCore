#!/usr/bin/env python
"""
Oracle implementation of AddRunLumi
"""

#This has been modified for Oracle

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddRunLumi import AddRunLumi as MySQLAddRunLumi

class AddRunLumi(MySQLAddRunLumi):

    sql = """INSERT INTO dbsbuffer_file_runlumi_map (filename, run, lumi, num_events)
            select id, :run, :lumi, :num_events from dbsbuffer_file
            where lfn = :lfn"""
