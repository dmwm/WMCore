#!/usr/bin/env python

"""
MySQL implementation of GetChecksum
"""





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
        if isinstance(dictVersion, list):
            if len(dictVersion) == 0:
                #Then it's empty
                return None
            else:
                #Otherwise there are several, and we have to record each one
                #I don't know how to do this yet.
                tmpDict = {}
                for entry in dictVersion:
                    tmpDict.update({entry.get('cktype', 'Default'): entry.get('cksum', None)})
                formattedResult['checksums']  = tmpDict
        else:
            formattedResult['checksums']  = {'Default': dictVersion.get('cksum', None)}
            if formattedResult == {'Default': None}:
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
