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
__revision__ = "$Id: New.py,v 1.6 2008/10/13 13:03:39 metson Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    Create a workflow ready for subscriptions
    """
    def getSQL(self, timestamp=None):
        sql = ""
        if timestamp == None:
            sql = """insert into wmbs_subscription 
                (fileset, workflow, type, split_algo) 
                values (:fileset, :workflow, :type, :split_algo)"""
        else:
            sql = """insert into wmbs_subscription 
                (fileset, workflow, type, last_update, split_algo) 
                values (:fileset, :workflow, :type, :timestamp, :split_algo)"""
        return sql

    def getBinds(self, **kwargs):
        binds = {}
        for i in kwargs.keys():
            if kwargs[i]:
                binds = self.dbi.buildbinds(self.dbi.makelist(kwargs[i]), i, binds)
        return binds
        
    def execute(self, fileset = None, workflow = None, 
                split = 'File', timestamp = None, type = 'Processing',\
                conn = None, transaction = False):
        binds = self.getBinds(fileset = fileset, 
                              workflow = workflow,
                              timestamp = timestamp, 
                              type = type,
                              split_algo = split)
        
        self.dbi.processData(self.getSQL(timestamp), binds, 
                         conn = conn, transaction = transaction)
        return True #or raise