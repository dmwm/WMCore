#!/usr/bin/env python
"""
Oracle implementation of GetRunLumiFile
"""




from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.GetRunLumiFile import GetRunLumiFile as MySQLGetRunLumiFile


class GetRunLumiFile(MySQLGetRunLumiFile):
    """
    Oracle implementation of GetRunLumiFile
    """
