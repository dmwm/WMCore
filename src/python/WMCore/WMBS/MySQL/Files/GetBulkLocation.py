#!/usr/bin/env python
"""
_GetBulkLocation_

MySQL implementation of File.GetBulkLocation
"""

__revision__ = "$Id: GetBulkLocation.py,v 1.3 2010/04/08 20:09:10 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetBulkLocation(DBFormatter):
    sql = """SELECT wmbs_location.se_name as site_name, :id as id  
               FROM wmbs_location
               WHERE wmbs_location.id IN (SELECT location FROM wmbs_file_location WHERE file = :id)
    """
    
    def getBinds(self, files=None):
        binds = []
        files = list(files)
        for f in files:
            binds.append({'id': f['id']})
        return binds

    def format(self, unformattedResult):
        #We need to assemble file:location pairs so that we have a dict
        #of all locations for a given file.

        #Use inbuilt methods to turn it into a dict
        result = self.formatDict(unformattedResult)

        fileDict = {}
        for entry in result:
            if not entry['id'] in fileDict.keys():
                fileDict[entry['id']] = []
            fileDict[entry['id']].append(entry['site_name'])

        return fileDict
    
    
    def execute(self, files=None, conn = None, transaction = False):

        if len(files) == 0:
            return {}
        
        binds = self.getBinds(files)

        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)

        return self.format(result)
