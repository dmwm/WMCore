#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the Oracle backend for the message
service.

"""

__revision__ = "$Id: Queries.py,v 1.1 2009/05/15 16:04:38 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@caltech.edu"

import threading

from WMCore.MsgService.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_

    This module implements the Oracle backend for the message
    service.
    
    """

    def showTables(self):
        """
        __showTables__

        Shows tables in database, used to filter out message queues and their
        buffers by matching patterns
        """

        result = self.execute("select * from tabs", {})
        return self.format(result)

    def lastInsertId(self, tableName):
        """
        __lastInsertId__

        Checks for last inserted id 
        """

        sqlStr = """
SELECT %s_seq.currval() FROM dual;
""" %(tableName)
        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]


    def insertMessageType(self, args ):
        """
        __insertMessageType__
 
        Inserts a new message type and returns ID
        """
        sqlStr = """
INSERT INTO ms_type(name) VALUES(:name)
"""
        
        self.execute(sqlStr, args)

        sqlStr2 = """
SELECT ms_type_seq.currval FROM dual
        """

        result = self.execute(sqlStr2, {})
        return self.formatOne(result)[0]


    def insertProcess(self, args):
        """
        __insertProcess__

        Inserts the name of the component in the backend and returns ID
        """

        sqlStr = """
INSERT INTO ms_process(host,pid,name) VALUES (:host,:pid,:name)
"""
        self.execute(sqlStr, args)

        sqlStr2 = """
SELECT ms_process_seq.currval FROM dual
        """

        result = self.execute(sqlStr2, {})
        return self.formatOne(result)[0]


    def setBufferState(self, args):
        """
        __setBufferState__

        Sets the state of a buffer
        """
        sqlStr = """
INSERT INTO ms_check_buffer(buffer, status) SELECT :buffername,:state FROM DUAL WHERE NOT EXISTS (SELECT buffer FROM ms_check_buffer WHERE buffer = :buffername)
"""
        self.execute(sqlStr,args)



#    def checkName(self, args):
#        """
#        __checkName__
#
#        Checks the name of the component in the backend.
#        """
#
#        sqlStr = """ 
#SELECT procid, host, pid FROM ms_process WHERE name = :name
#"""
#        print sqlStr
#        print args
#        result = self.execute(sqlStr, args)
#        return self.formatOneDict(result)
