#!/usr/bin/env python
"""
_BulkNewReturn_

Oracle implementation of JobGroup.BulkNewReturn
"""

__revision__ = "$Id: BulkNewReturn.py,v 1.2 2010/03/08 16:31:14 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import time

from WMCore.WMBS.MySQL.JobGroup.BulkNewReturn import BulkNewReturn as MySQLBulkNewReturn

class BulkNewReturn(MySQLBulkNewReturn):
    """
    Does a bulk commit of jobGroups, followed by returning their IDs and UIDs

    """
    sql = """INSERT INTO wmbs_jobgroup (id, subscription, guid, output,
              last_update) VALUES (wmbs_jobgroup_SEQ.nextval, :subscription,
              :guid, :output, %d)""" % time.time()

    returnSQL = """SELECT id AS id, guid AS guid FROM wmbs_jobgroup
                   WHERE subscription = :subscription
                   AND guid = :guid
                   AND output = :output"""
