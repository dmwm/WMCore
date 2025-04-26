#!/usr/bin/env python

"""
Oracle implementation of AddCKType
"""





from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.AddCKType import AddCKType as MySQLAddCKType

class AddCKType(MySQLAddCKType):
    sql = """INSERT INTO dbsbuffer_checksum_type (type)
             VALUES (:cktype)"""
