#!/usr/bin/env python
"""
_AcquireFiles_

Oracle implementation of Subscription.GetAvailableFiles

Return a list of files that are available for processing.
Available means not acquired, complete or failed.
"""
__all__ = []
__revision__ = "$Id: GetAvailableFiles.py,v 1.7 2009/03/16 16:58:38 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WMBS.MySQL.Subscriptions.GetAvailableFiles import \
     GetAvailableFiles as GetAvailableFilesMySQL

class GetAvailableFiles(GetAvailableFilesMySQL):
    
    def getSQLAndBinds(self, subscription, maxFiles, conn = None,
                       transaction = None):
        sql = ""
        binds = {'subscription': subscription}
        
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

        binds = {"subscription": subscription, "maxfiles": maxFiles}
        
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
            and rownum <= :maxfiles    
            """
            
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
            and rownum <= :maxfiles    
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
                (select fileid from wmbs_file_location)
            and rownum <= :maxfiles
            """
                
        return sql, binds
