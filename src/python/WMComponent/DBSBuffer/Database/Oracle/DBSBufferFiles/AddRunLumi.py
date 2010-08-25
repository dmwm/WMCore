#!/usr/bin/env python
"""
Oracle implementation of AddRunLumi
"""

#This has been modified for Oracle

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddRunLumi import AddRunLumi as MySQLAddRunLumi

class AddRunLumi(MySQLAddRunLumi):

    sql = """INSERT INTO dbsbuffer_file_runlumi_map (filename, run, lumi) 
            select id, :run, :lumi from dbsbuffer_file
            where lfn = :lfn"""





