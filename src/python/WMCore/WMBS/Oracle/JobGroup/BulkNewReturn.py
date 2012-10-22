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

    sql = """INSERT INTO wmbs_jobgroup (id, subscription, guid, output,
                      last_update) VALUES (wmbs_jobgroup_SEQ.nextval, :subscription,
                      :guid, :output, :timestamp)"""
