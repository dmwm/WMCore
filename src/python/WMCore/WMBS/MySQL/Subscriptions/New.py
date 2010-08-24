#!/usr/bin/env python
"""
_Subscription.New_

MySQL implementation of Subscription.New
"""




import time
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    Create a workflow ready for subscriptions
    """
    sql = """INSERT INTO wmbs_subscription (fileset, workflow, subtype,
                                            split_algo, last_update) 
               SELECT :fileset, :workflow, id, :split_algo, :timestamp
                      FROM wmbs_sub_types WHERE name = :subtype""" 
    
    def execute(self, fileset = None, workflow = None, split_algo = "File",
                type = "Processing", conn = None, transaction = False):
        binds = {"fileset": fileset, "workflow": workflow, "subtype": type,
                 "split_algo": split_algo, "timestamp": int(time.time())}
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
