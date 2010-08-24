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
__revision__ = "$Id: New.py,v 1.8 2008/11/25 17:21:13 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    Create a workflow ready for subscriptions
    """
    
    sql = """insert into wmbs_subscription 
                (fileset, workflow, type, split_algo, last_update) 
                values (:fileset, :workflow, :type, :split_algo, unix_timestamp())"""
    
    def getBinds(self, **kwargs):
        binds = {}
        for i in kwargs.keys():
            if kwargs[i]:
                binds = self.dbi.buildbinds(self.dbi.makelist(kwargs[i]), i, binds)
        return binds
        
    def format(self, result):
        return True
    
    def execute(self, fileset = None, workflow = None, 
                split = 'File', type = 'Processing',
                conn = None, transaction = False):
        binds = self.getBinds(fileset = fileset, 
                              workflow = workflow, 
                              type = type,
                              split_algo = split)
        
        self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return True #or raise
