#!/usr/bin/env python
"""
_AvailableFiles_

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
__revision__ = "$Id: GetAvailableFiles.py,v 1.9 2009/02/03 20:44:21 sryu Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetAvailableFiles(DBFormatter):
    def getSQLAndBinds(self, subscription, conn = None, transaction = None):
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
                        valid = 1)
                )
                """
            binds = {'subscription': subscription}
            
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
                        valid = 0)
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
                (select file from wmbs_sub_files_complete where subscription=:subscription)
            and file in
                (select file from wmbs_file_location)"""
                
        return sql, binds

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming "fileid" to file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "file" in formattedResult.keys():
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        return formattedResults
           
    def execute(self, subscription = None, conn = None, transaction = False):
        sql, binds = self.getSQLAndBinds(subscription, conn = conn,
                                         transaction = transaction)
        results = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(results)
