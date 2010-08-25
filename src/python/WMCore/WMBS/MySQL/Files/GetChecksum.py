#!/usr/bin/env python

"""
MySQL implementation of GetChecksum
"""


__revision__ = "$Id: GetChecksum.py,v 1.1 2009/12/02 19:34:36 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetChecksum(DBFormatter):
    sql = """SELECT cst.type AS cktype, fcs.cksum AS cksum FROM
               wmbs_file_checksums fcs INNER JOIN
               wmbs_checksum_type cst
               ON fcs.typeid = cst.id
               WHERE fcs.fileid = :fileid"""

    def formatResult(self, result):
        """
        I need the result in a reasonable list.
        This will return None if there is no cksum

        """
        formattedResult = {}

        dictVersion = DBFormatter.formatDict(self, result)
        if type(dictVersion) == type([]):
            if len(dictVersion) == 0:
                #Then it's empty
                return None
            elif len(dictVersion) == 1:
                #There's only one entry
                formattedResult['cktype'] = dictVersion[0].get('cktype', None)
                formattedResult['cksum']  = dictVersion[0].get('cksum', None)
            else:
                #Otherwise there are several, and we have to record each one
                #I don't know how to do this yet.
                tmpList = []
                for entry in dictVersion:
                    tmpList.append({'cktype': entry.get('cktype', None), 'cksum': entry.get('cksum', None)})
                formattedResult['cktype'] = tmpList
                formattedResult['cksum']  = tmpList
        else:
            formattedResult['cktype'] = dictVersion.get('cktype', None)
            formattedResult['cksum']  = dictVersion.get('cksum', None)
            if formattedResult == {'cktype': None, 'cksum': None}:
                #Then the thing was empty anyway
                return None

        return formattedResult
                
                
    def execute(self, fileid = None, bulkList = None, conn = None, transaction = False):

        if bulkList:
            #Would need to accept a bulk list of form [{fileid: fileid}]
            binds = bulkList
        else:
            binds = {'fileid': fileid}

        result = self.dbi.processData(self.sql, binds, 
                                      conn = conn, transaction = transaction)

        return self.formatResult(result)
    
