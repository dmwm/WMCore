#!/usr/bin/env python
"""
_AcquireFiles_

SQLite implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.4 2008/11/24 22:10:45 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    
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
            if i[0] > 0 and i[1] == '0':
                blacklist = True
            elif i[0] > 0 and i[1] == '1':
                whitelist = True
        
        if whitelist:
            sql = """select fileid from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_acquired where subscription=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_failed where subscription=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_complete where subscription=:subscription)
            and fileid in
                (select fileid from wmbs_file_location where location in
                    (select location from wmbs_subscription_location where
                        subscription=:subscription and
                        valid = 1)
                )
                """
            binds = {'subscription': subscription['id']}
            
        elif blacklist:
            sql = """select fileid from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_acquired where subscription=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_failed where subscription=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_complete where subscription=:subscription)
            and fileid in
                (select fileid from wmbs_file_location where location not in
                    (select location from wmbs_subscription_location where
                        subscription=:subscription and
                        valid = 0)
                )
                """
                
        else:
            sql = """select fileid from wmbs_fileset_files where
            fileset = (select fileset from wmbs_subscription where id=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_acquired where subscription=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_failed where subscription=:subscription)
            and fileid not in 
                (select fileid from wmbs_sub_files_complete where subscription=:subscription)
            and fileid in
                (select fileid from wmbs_file_location)"""
                
        return sql, binds