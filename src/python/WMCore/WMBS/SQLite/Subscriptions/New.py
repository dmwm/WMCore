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
__revision__ = "$Id: New.py,v 1.4 2008/11/25 17:19:28 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    """
    Create a workflow ready for subscriptions
    """
    sql = """insert into wmbs_subscription 
                (fileset, workflow, type, last_update, split_algo) 
                values (:fileset, :workflow, :type, strftime('%s', 'now'), :split_algo)"""
        
    def execute(self, fileset = None, workflow = None, 
                split = 'File', timestamp = None, type = 'Processing',\
                    spec = None, owner = None, conn = None, transaction = False):
        if not timestamp:
            timestamp = self.timestamp()
        binds = self.getBinds(fileset = fileset, 
                              workflow = workflow,
                              timestamp = timestamp, 
                              type = type,
                              spec = spec, 
                              owner = owner,
                              split_algo = split)
        
        self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return True #or raise
