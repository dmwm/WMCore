#!/usr/bin/env python

"""
This should load the files from active blocks with
the block info
"""

from WMComponent.DBSUpload.Database.MySQL.LoadFilesFromBlocks import LoadFilesFromBlocks as MySQLLoadFilesFromBlocks

class LoadFilesFromBlocks(MySQLLoadFilesFromBlocks):
    """
    Oracle version

    """
