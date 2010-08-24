#!/usr/bin/env python
"""
_Subscription.New_

MySQL implementation of Subscription.New

TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.3 2008/06/24 16:57:41 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class New(MySQLBase):
    """
    Create a workflow ready for subscriptions
    """
    def getSQL(self, timestamp=None):
        sql = ""
        if timestamp == None:
            sql = """insert into wmbs_subscription (fileset, workflow, type) 
                values (:fileset, :workflow, :type)"""
        else:
            sql = """insert into wmbs_subscription (fileset, workflow, type, last_update) 
                values (:fileset, :workflow, :type, :timestamp)"""
        return sql
        
    def execute(self, fileset = None, workflow = None, timestamp = None, type = 'Processing',\
                    spec = None, owner = None, conn = None, transaction = False):
        binds = self.getBinds(fileset = fileset, 
                              workflow = workflow,
                              timestamp = timestamp, 
                              type = type,
                              spec = spec, 
                              owner = owner)
        
        self.dbi.processData(self.getSQL(timestamp), binds, 
                         conn = conn, transaction = transaction)
        return True #or raise