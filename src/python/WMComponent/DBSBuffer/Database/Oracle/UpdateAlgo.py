#!/usr/bin/env python
"""
_DBSBuffer.UpdateAlgo_

Add PSetHash to Algo in DBS Buffer

"""
__revision__ = "$Id: UpdateAlgo.py,v 1.1 2009/05/15 16:19:13 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

from WMComponent.DBSBuffer.Database.MySQL.UpdateAlgo import UpdateAlgo as MySQLUpdateAlgo

class UpdateAlgo(MySQLUpdateAlgo):

    sql = """UPDATE dbsbuffer_algo
                SET PSetHash=:psetHash 
                WHERE
                AppName=:exeName
                AND AppVer=:appVersion
                AND AppFam=:appFamily
                AND ID = 
                    (select Algo FROM dbsbuffer_dataset WHERE Path = :path)
                """
    
    def execute(self, dataset=None, psethash=None, conn=None, transaction = False):
        binds = self.getBinds(dataset, psethash)
        try:
            result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        except Exception, ex:
            if ex.__str__().find("unique") != -1 :
                #Disregard duplicate entry
                return
            else:
                raise ex

        return 


