#!/usr/bin/env python
"""
_Queries_

This module implements the Oracle backend for the message
service.
"""




import threading

from WMCore.MsgService.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_

    This module implements the Oracle backend for the message
    service.
    
    """
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

    def msgArrived(self, args):
        """
        __msgArrived__

        Sets the flag in a small table (metadata) that a message has arrived for
        a component so that component does not need to check a big table for
        this.

        Note: The MySQL version of this query makes use of the
        "ON DUPLICATE KEY UPDATE" qualifer on the INSERT statement which is not
        supported in Oracle.  We'll instead do an UPDATE and then an INSERT
        to achieve the same functionality.
        """
        updateSql = "UPDATE %s SET status = 'there' WHERE procid = :procid" % \
                    (args["table"])
        insertSql = """INSERT INTO %s(procid) SELECT :procid AS procid FROM DUAL
                         WHERE NOT EXISTS
                           (SELECT procid FROM %s WHERE procid = :procid)""" % \
                    (args["table"], args["table"])

        binds = []
        for dest in args["msgs"].keys():
            binds.append({"procid": dest})

        self.execute(updateSql, binds)
        self.execute(insertSql, binds)
        return

    def showTables(self):
        """
        __showTables__

        Shows tables in the database, used to filter out message queues and
        their buffers by matching parameters.
        """
        result = self.execute("select table_name from tabs", {})
        formattedResult = self.format(result)

        lowerCaseNames = []
        for row in formattedResult:
            lowerRow = []
            for column in row:
                lowerRow.append(column.lower())
                                
            lowerCaseNames.append(lowerRow)

        return lowerCaseNames

    def getMsg(self, args):
        """
        _getMsg_

        Gets the actual messages keeping in mind the possible delays.

        Note: Oracle doesn't support LIMIT like MySQL does so we'll use the
        rownum pseudo column instead.
        """
        sqlStr = """SELECT * FROM
                      (SELECT %s.messageid as messageid, ms_type.name as name,
                              %s.payload as payload, ms_process.name as source
                         FROM %s, ms_type,ms_process
                       WHERE ms_type.typeid = %s.type and ms_process.procid = %s.source
                         AND %s.time + TO_DSINTERVAL(CONCAT('0 ',%s.delay)) <= CURRENT_TIMESTAMP
                         AND %s.dest=:procid
                       ORDER BY time,messageid)
                    WHERE ROWNUM <= 1""" % \
          (args["table"], args["table"], args["table"], args["table"],
           args["table"], args["table"], args["table"], args["table"])

        result = self.execute(sqlStr, {"procid": args["procid"]})
        result = self.formatOneDict(result)

        loweredResult = {}
        for keyName in result.keys():
            loweredResult[keyName.lower()] = result[keyName]
            
        return loweredResult

    def moveMsgFromBufferIn(self, args):
        """
        __moveMsg__

        Moves message from one table to another.

        Note: The delete statement has been changed because Oracle
        doesn't support the limited delete statement.
        """
        sqlStr1 = """INSERT INTO %s(type, source, dest, payload, time, delay)
                       SELECT * FROM
                         (SELECT type, source, dest, payload, time, delay
                           FROM %s ORDER BY messageid)
                       WHERE ROWNUM <= %s""" % \
          (str(args["target"]), str(args["source"]), str(args["limit"]))

        sqlStr2 = """DELETE FROM %s WHERE messageid IN
                       (SELECT * FROM
                          (SELECT messageid FROM %s ORDER BY messageid)
                        WHERE ROWNUM <= %s)""" % \
          (str(args["source"]), str(args["source"]), str(args["limit"]))

        self.execute(sqlStr1, {})
        self.execute(sqlStr2, {})        
        return

    def moveMsgToBufferOut(self, args):
        """
        _moveMsgToBufferOut_

        Moves messages from the buffer in or the main queueu to buffer out.

        Note: Oracle handles dates differently, which is why we need an
        Oracle specific query here.
        """
        sqlStr1 = """INSERT INTO %s(type, source, dest, payload, delay, time)
                       SELECT * FROM
                         (SELECT type, source, dest, payload, delay, time FROM %s 
                            WHERE dest = :procid
                              AND %s.time + TO_DSINTERVAL(CONCAT('0 ',%s.delay)) <= CURRENT_TIMESTAMP
                            ORDER BY messageid)
                       WHERE ROWNUM <= %s""" % \
          (args["target"], args["source"], args["source"], args["source"],
           args["buffer_size"])

        sqlStr2 = """DELETE FROM %s WHERE messageid IN
                       (SELECT * FROM
                         (SELECT messageid FROM %s
                            WHERE dest = :procid AND
                              %s.time + TO_DSINTERVAL(CONCAT('0 ', %s.delay)) <= CURRENT_TIMESTAMP
                            ORDER BY messageid)
                         WHERE ROWNUM <= %s)""" % \
          (args["source"], args["source"], args["source"], args["source"],
           args["buffer_size"])

        self.execute(sqlStr1, {"procid": args["procid"]})
        self.execute(sqlStr2, {"procid": args["procid"]})
        return
