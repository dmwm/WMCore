#!/usr/bin/env python

"""
Oracle implementation of AddCKType
"""





from WMCore.WMBS.MySQL.Files.AddCKType import AddCKType as MySQLAddCKType

class AddCKType(MySQLAddCKType):
    sql = """INSERT INTO wmbs_checksum_type (type)
                VALUES (:cktype)"""
