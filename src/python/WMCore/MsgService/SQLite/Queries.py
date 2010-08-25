#!/usr/bin/env python
#FIXME: there are many commit statements
# in these methods. Perhaps they can be factored out.
# but be careful this can lead to deadlock exceptions.
"""
_Queries_

This module implements the SQLite backend for the message
service through the MySQL version
"""




from WMCore.MsgService.MySQL.Queries import Queries as MySQLQueries

class Queries(MySQLQueries):
    """
    _Queries_
    
    This module implements the SQLite backend for the message
    service through the MySQL version
    """
    def lastInsertId(self):
        """
	__lastInsertId__
	
	Checks for last inserted id 
	"""
        sqlStr = "SELECT LAST_INSERT_ROWID()"

        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]


    def setBufferState(self, args):
        """
	__setBufferState__
		
	Sets the state of a buffer
	"""
	#Changed to INSERT OR IGNORE from MySQL INSERT IGNORE
	#Should have the same functionality.
	#TODO: Should this be REPLACE?
	# -mnorman
        sqlStr = """
INSERT OR IGNORE INTO ms_check_buffer(buffer, status) VALUES(:buffername,:state)
                 """
        self.execute(sqlStr, args)

    def getBufferState(self, args):
        """
	__bufferState__
	
	Returns the state of the buffer using 
	"""
	#FOR UPDATE portion removed: SQLite claims that row locking is
	#redundant in their data structure.
	#TODO: Test that
	# -mnorman
        
        if len(args) == 0:
            return []
        if len(args) == 1:
            sqlStr = """
SELECT buffer, status FROM ms_check_buffer WHERE buffer='%s' """ % (str(args[0]))
        else:
            sqlStr = """
SELECT buffer, status FROM ms_check_buffer WHERE buffer IN %s """ % (str(tuple(args)))

        result = self.execute(sqlStr, {})
        return self.format(result)

    def msgArrived(self, args):
        """
        __msgArrived__

        Sets the flag in a small table (metadata) that a message has arrived for
        a component so that component does not need to check a big table for
        this.

        Note: The MySQL version of this query makes use of the
        "ON DUPLICATE KEY UPDATE" qualifer on the INSERT statement which is not
        supported in SQLite.  We'll instead do an UPDATE and then an INSERT
        to achieve the same functionality.
        """
        updateSql = "UPDATE %s SET status = 'there' WHERE procid = :procid" % \
                    (args["table"])
        insertSql = """INSERT INTO %s(procid) SELECT :procid AS procid
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
        result = self.execute("select name from main.sqlite_master where type = 'table'", {})
        return self.format(result)

    def getMsg(self, args):
        """
        _getMsg_

        Gets the actual messages keeping in mind the possible delays.

        Note: The MySQL version of this query uses the "ADDTIME" function to
        tack the delay onto the message's timestamp.  We'll have to use the
        SQLite datetime() function to do the same thing.
        """
        sqlStr = """SELECT %s.messageid as messageid, ms_type.name as name,
                           %s.payload as payload, ms_process.name as source
                      FROM %s, ms_type,ms_process
                    WHERE ms_type.typeid = %s.type and ms_process.procid = %s.source
                      AND datetime(%s.time, %s.delay) <= CURRENT_TIMESTAMP
                      AND %s.dest=:procid
                    ORDER BY time,messageid LIMIT 1""" % \
          (args["table"], args["table"], args["table"], args["table"],
           args["table"], args["table"], args["table"], args["table"])

        result = self.execute(sqlStr, {"procid": args["procid"]})
        result = self.formatOneDict(result)
        return result

    def moveMsgFromBufferIn(self, args):
        """
        __moveMsg__

        Moves message from one table to another.

        Note: The delete statement has been changed because my SQLite
        installation doesn't support the limited delete statement.
        """
        sqlStr1 = """INSERT INTO %s(type, source, dest, payload, time, delay) 
                       SELECT type, source, dest, payload, time, delay
                         FROM %s ORDER BY messageid LIMIT %s""" % \
          (str(args["target"]), str(args["source"]), str(args["limit"]))

        sqlStr2 = """DELETE FROM %s WHERE messageid IN
                       (SELECT messageid FROM %s
                         ORDER BY messageid LIMIT %s)""" % \
          (str(args["source"]), str(args["source"]), str(args["limit"]))

        self.execute(sqlStr1, {})
        self.execute(sqlStr2, {})        
        return

    def moveMsgToBufferOut(self, args):
        """
        _moveMsgToBufferOut_

        Moves messages from the buffer in or the main queueu to buffer out.

        Note: The MySQL version of this query uses the "ADDTIME" function to
        tack the delay onto the message's timestamp.  We'll have to use the
        SQLite datetime() function to do the same thing.  The delete statement
        has also been changed because my SQLite installation doesn't support
        the limited delete statement.
        """
        sqlStr1 = """INSERT INTO %s(type, source, dest, payload, delay, time) 
                       SELECT type, source, dest, payload, delay, time FROM %s 
                         WHERE dest = :procid
                           AND datetime(%s.time, %s.delay) <= CURRENT_TIMESTAMP
                        ORDER BY messageid LIMIT %s""" % \
          (args["target"], args["source"], args["source"], args["source"],
           args["buffer_size"])

        sqlStr2 = """DELETE FROM %s WHERE messageid IN
                       (SELECT messageid FROM %s
                          WHERE dest = :procid AND
                            datetime(%s.time, %s.delay) <= CURRENT_TIMESTAMP
                          ORDER BY messageid LIMIT %s)""" % \
          (args["source"], args["source"], args["source"], args["source"],
           args["buffer_size"])

        self.execute(sqlStr1, {"procid": args["procid"]})
        self.execute(sqlStr2, {"procid": args["procid"]})
        return
