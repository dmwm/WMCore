#!/usr/bin/env python
"""
_BulkNewReturn_

Oracle implementation of JobGroup.BulkNewReturn
"""




from WMCore.WMBS.MySQL.JobGroup.BulkNewReturn import BulkNewReturn as MySQLBulkNewReturn

class BulkNewReturn(MySQLBulkNewReturn):
    """
    Does a bulk commit of jobGroups, followed by returning their IDs and UIDs

    """
