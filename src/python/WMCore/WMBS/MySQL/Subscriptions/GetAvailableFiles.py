#!/usr/bin/env python
"""
_AcquireFiles_

MySQL implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed and takes into account site 
black/white lists.

CREATE TABLE wmbs_file_location (
             file     INT(11),
             location INT(11),
             UNIQUE(file, location),
             FOREIGN KEY(file)     REFERENCES wmbs_file_details(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location) REFERENCES wmbs_location(id)
               ON DELETE CASCADE)

CREATE TABLE wmbs_subscription_location (
             subscription     INT(11)      NOT NULL,
             location         INT(11)      NOT NULL,
             valid            BOOLEAN      NOT NULL DEFAULT TRUE,
             FOREIGN KEY(subscription)  REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location)     REFERENCES wmbs_location(id)
               ON DELETE CASCADE)"
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.3 2008/11/11 14:02:05 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class GetAvailableFiles(MySQLBase):
#    sql = """select lfn from wmbs_file_details
#                where id in (select file from wmbs_fileset_files where
#            fileset = (select fileset from wmbs_subscription where id=:subscription)
#            and file not in 
#                (select file from wmbs_sub_files_acquired where subscription=:subscription)
#            and file not in 
#                (select file from wmbs_sub_files_failed where subscription=:subscription)
#            and file not in 
#                (select file from wmbs_sub_files_complete where subscription=:subscription)
#                )
#        """
    def getSQLAndBinds(self, subscription, conn = None, transaction = None):
        sql = ""
        binds = {'subscription': subscription['id']}
        
        sql = '''select count(valid), valid from wmbs_subscription_location
        where subscription = :subscription group by valid'''
        result = self.dbi.processData(sql, binds, 
                                      conn = conn, transaction = transaction)
        result = self.format(result)
        
        whitelist = False
        blacklist = False
        
        for i in result:
            # i is a tuple with count and valid, 0 = False
            if i[0] > 0 and i[1] == 0:
                blacklist = True
            elif i[0] > 0 and i[1] == 1:
                whitelist = True
        
        if whitelist:
            sql = """select file from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and file not in 
                (select file from wmbs_sub_files_acquired where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_complete where subscription=:subscription)
            and file in
                (select file from wmbs_file_location where location in
                    (select location from wmbs_subscription_location where
                        subscription=:subscription and
                        valid = TRUE)
                )
                """
            binds = {'subscription': subscription['id']}
            
        elif blacklist:
            sql = """select file from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and file not in 
                (select file from wmbs_sub_files_acquired where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_complete where subscription=:subscription)
            and file in
                (select file from wmbs_file_location where location not in
                    (select location from wmbs_subscription_location where
                        subscription=:subscription and
                        valid = FALSE)
                )
                """
                
        else:
            sql = """select file from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and file not in 
                (select file from wmbs_sub_files_acquired where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_failed where subscription=:subscription)
            and file not in 
                (select file from wmbs_sub_files_complete where subscription=:subscription)"""
                
        return sql, binds
           
    def execute(self, subscription=None, conn = None, transaction = False):
        sql, binds = self.getSQLAndBinds(subscription, conn = conn, transaction = transaction)
        
        
        
        result = self.dbi.processData(sql, binds, 
                                      conn = conn, transaction = transaction)
        return self.format(result)

