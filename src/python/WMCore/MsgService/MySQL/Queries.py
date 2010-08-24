#!/usr/bin/env python
#pylint: disable-msg=E1103

#FIXME: there are many commit statements
# in these methods. Perhaps they can be factored out.
# but be careful this can lead to deadlock exceptions.
"""
_Queries_

This module implements the mysql backend for the message
service.

"""



import threading

from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the mysql backend for the message
    service.
    
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def checkName(self, args):
        """
        __checkName__

        Checks the name of the component in the backend.
        """

        sqlStr = """ 
SELECT procid, host, pid FROM ms_process WHERE name = :name 
"""
        result = self.execute(sqlStr, args)
        return self.formatOneDict(result)

    def updateName(self, args):
        """
        __updateName__

        Updates the name of the component in the backend.
        """

        sqlStr = """
UPDATE ms_process SET pid = :currentPid, host = :currentHost WHERE name = :name
"""
        self.execute(sqlStr, args)

    def insertProcess(self, args):
        """
        __insertProcess__

        Inserts the name of the component in the backend
        """

        sqlStr = """
INSERT INTO ms_process(host,pid,name) VALUES (:host,:pid,:name)
"""
        self.execute(sqlStr, args)

    def lastInsertId(self):
        """
        __lastInsertId__

        Checks for last inserted id 
        """

        sqlStr = """
SELECT LAST_INSERT_ID()
"""
        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]

    def checkMessageType(self, args):
        """
        __checkMessageType__
 
        Checks if the name for a message is already registered in the database
        """
        sqlStr = """
SELECT typeid,name FROM ms_type WHERE name = :name """ 
        result = self.execute(sqlStr, args)
        return self.formatOneDict(result)

    def insertMessageType(self, args ):
        """
        __insertMessageType__
 
        Inserts a new message type
        """
        sqlStr = """
INSERT INTO ms_type(name) VALUES(:name)
"""
        self.execute(sqlStr, args)

    def checkSubscription(self, args ):
        """

        __checkSubscription__

        Checks if a component is already subscribed
        """
        sqlStr = """
SELECT procid, typeid FROM ms_subscription WHERE procid = :procid 
AND typeid = :typeid
""" 
        result = self.execute(sqlStr, args)
        return self.formatOneDict(result)

    def insertSubscription(self, args):
        """
        __insertSubscription__

        Inserts a subscription to a message.
        """

        sqlStr = """
INSERT INTO ms_subscription(procid,typeid) VALUES(:procid,:typeid)
"""
        self.execute(sqlStr, args)

    def checkPrioritySubscription(self, args):
        """

        __checkPrioritySubscription__

        Checks if a component is already subscribed
        """
        sqlStr = """
SELECT procid, typeid FROM ms_subscription_priority WHERE procid = :procid 
AND typeid = :typeid
""" 
        result = self.execute(sqlStr, args)
        return self.formatOneDict(result)

    def insertPrioritySubscription(self, args):
        """
        __insertPrioritySubscription__

        Inserts a subscription to a message.
        """

        sqlStr = """
INSERT INTO ms_subscription_priority(procid,typeid) VALUES(:procid,:typeid)
"""
        self.execute(sqlStr, args)

    def subscriptions(self, args):
        """
        __subscriptions__

        Returns a list (array) of subscriptions
        """
        sqlStr = """ 
SELECT ms_type.name FROM ms_subscription,ms_type WHERE procid = :procid 
AND ms_subscription.typeid = ms_type.typeid 
        """
        result = self.execute(sqlStr, args)
        return self.format(result)

    def prioritySubscriptions(self, args):
        """
        __prioritySubscriptions__

        Returns a list (array) of subscriptions
        """
        sqlStr = """ 
SELECT ms_type.name FROM ms_subscription_priority,ms_type WHERE procid = :procid 
AND ms_subscription_priority.typeid = ms_type.typeid 
        """
        result = self.execute(sqlStr, args)
        return self.format(result)

    def getDestinations(self, args):
        """
        __getDestinations__

        Find out who are the receivers of your published message.
        """

        sqlStr = """
SELECT ms_subscription.procid,ms_process.name FROM ms_subscription, ms_process 
WHERE ms_subscription.typeid = :typeid AND 
ms_subscription.procid = ms_process.procid
 """ 
        result = self.execute(sqlStr, args)
        return self.format(result)

    def getPriorityDestinations(self, args):
        """
        __getPriorityDestinations__

        Find out who are the receivers of your published message.
        """

        sqlStr = """
SELECT ms_subscription_priority.procid,ms_process.name FROM ms_subscription_priority, ms_process 
WHERE ms_subscription_priority.typeid = :typeid AND 
ms_subscription_priority.procid = ms_process.procid
""" 
        result = self.execute(sqlStr, args)
        return self.format(result)

    def initializeAvailable(self, args):
        """
        __initializeArrive__

        initializes meta data on arriving messages.
        """
        sqlStr1 = """
INSERT INTO ms_available(procid) VALUES(:procid)
"""
        sqlStr2 = """
INSERT INTO ms_available_priority(procid) VALUES(:procid)
"""
        self.execute(sqlStr1, args)        
        self.execute(sqlStr2, args)        

    def msgAvailable(self, args):
        """"
        __msgAvailable__

        Sets meta data table that there are messages in the queue.
        """

        sqlStr = """
SELECT status FROM %s WHERE procid = :procid
""" % (args['table'])
        result = self.execute(sqlStr, {'procid': args['procid']})
        return self.formatOne(result) 

    def noMsgs(self, args):
        """
        _noMsgs_

        Sets meta data table that there are no messages.
        """
        sqlStr = """
UPDATE %s SET status='not_there' WHERE procid=:procid 
        """ % (args['table'])
        self.execute(sqlStr, {'procid': args['procid']})

    def msgArrived(self, args):
        """
        __msgArrived__

        sets the flag in a small table (metadata) that
        a message has arrived for a component so the component
        does not need to check a big table for this.
        """
        sqlStr = """
INSERT INTO %s(procid) VALUES(:procid) ON DUPLICATE KEY UPDATE status = 'there'
""" % (args['table'])

        # format for bind input
        target = []
        for dest in args['msgs'].keys():
            target.append({'procid':dest})
        self.execute(sqlStr, target)

    def insertMsg(self, args):
        """
        __insertMsg__

        inserts messages in specific (buffer) tables.
        """

        sqlStr = """
INSERT INTO %s(type,source,dest,payload,delay) VALUES(:type,:source,:dest,:payload,:delay)
""" % (args['table'])

        # we need to cut things up as mysql can not deal with very large 
        # inserts (over 500). We are conservative and stop at 100
        # FIXME: we would like to have this chopping logic 
        # FIXME: in a high level formatter.
        if len(args['msgs'])>100:
            start = 0
            end = 100
            while start < len(args['msgs']):
                if end > len(args['msgs']):
                    end = len(args['msgs']) 
                self.execute(sqlStr, args['msgs'][start:end])
                start += 100
                end += 100 
            return
        self.execute(sqlStr, args['msgs'])

    def processMsg(self, args):
        """
        __processMsg__

        Sets the state of a message in the queue to 'processing' which
        happens when it is being processed by a component.
        """

        sqlStr = """
UPDATE %s SET state='processing' WHERE messageid = :msgId
""" % (args['table'])
        self.execute(sqlStr, {'msgId':args['msgId']})

    def removeMsg(self, args):
        """
        __removeMsg__
  
        Removes a message from a queue table (table is configurable).
        """

        sqlStr = """
DELETE FROM %s WHERE messageid = :msgId 
""" % (args['table'])
        self.execute(sqlStr, {'msgId':args['msgId']})
     

    def getMsg(self, args):
        """
        _getMsg_

        Gets the actual messages keeping in mind possible delays.
        """
        myThread = threading.currentThread()

        sqlStr = """
SELECT %s.messageid as messageid, ms_type.name as name, %s.payload as payload,
 ms_process.name as source FROM %s, ms_type,ms_process
WHERE ms_type.typeid=%s.type and  ms_process.procid=%s.source 
AND ADDTIME(%s.time,%s.delay) <= CURRENT_TIMESTAMP and
%s.dest=:procid ORDER BY time,messageid LIMIT 1 """ % (args['table'], \
        args['table'], args['table'], args['table'], args['table'], \
        args['table'], args['table'], args['table'])
        myThread.transaction.begin()
        result = self.execute(sqlStr, {'procid':args['procid']})
        result = self.formatOneDict(result)
        myThread.transaction.commit()
        return result

    def insertComponentMsgTables(self, componentName):
        """
        __insertComponetMsgTables__
 
        Inserts tables for components when in multiQueue mode.
        Each component will have its own buffers and queu tables.
        """

        prefix1 = 'ms_message_'+componentName
        prefix2 = 'ms_priority_message_'+componentName

        for prefix in [prefix1, prefix2]:
            for postfix in ['', '_buffer_in']:
                tableName = prefix+postfix
                sqlStr = """
CREATE TABLE `%s` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',

   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
""" % (tableName)
                self.execute(sqlStr, {})
        for prefix in [prefix1, prefix2]:
            postfix = '_buffer_out'
            tableName = prefix+postfix
            sqlStr = """
CREATE TABLE `%s` (
   `messageid` int(11) NOT NULL auto_increment,
   `type` int(11) NOT NULL default '0',
   `source` int(11) NOT NULL default '0',
   `dest` int(11) NOT NULL default '0',
   `payload` text NOT NULL,
   `time` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
   `delay` varchar(50) NOT NULL default '00:00:00',
   `state` enum('wait', 'processing','finished') default 'wait',


   PRIMARY KEY `messageid` (`messageid`),
   FOREIGN KEY(`type`) references `ms_type`(`typeid`),
   FOREIGN KEY(`source`) references `ms_process`(`procid`),
   FOREIGN KEY(`dest`) references `ms_process`(`procid`)
   ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
""" % (tableName)
            self.execute(sqlStr, {})

    def tableSize(self, args):
        """
        __tableSize__

        returns the table size to deal with buffer movements.
        """
        sqlStr = """
SELECT COUNT(*) FROM %s """  % (args)
        result = self.execute(sqlStr, {})
        result = self.formatOne(result)[0]
        return result

    def showTables(self):
        """
        __showTables__

        Shows tables in database, used to filter out message queues and their
        buffers by matchin patterns
        """

        result = self.execute("show tables", {})
        return self.format(result)

    def purgeTable(self, tableName):
        """
        __purgeTable__
 
        Purges a table (used for message queues and buffers).
        """
        # some messages in the buffer_out tables
        # might being processed so we do not want
        # to delete them.
        if tableName.rfind('_buffer_out') >= 0:
            sqlStr = """
DELETE FROM %s WHERE state<>'processing'
""" % (tableName)
        else: 
            sqlStr = """
DELETE FROM %s 
""" % (tableName)
        self.execute(sqlStr, {})

    def inQueue(self, args):
        """
        __inQueue__
   
        Checks if a certain message is in a queue.
        """

        sqlStr = """
SELECT COUNT(*) FROM %s WHERE type = :typeid
""" % (args['tableName'])

        result = self.execute(sqlStr, args['sqlArgs'])
        result = self.formatOne(result)
        return result[0]

    def inQueueForComponent(self, args):
        """
        __inQueue__
   
        Checks if there are messages for a component in the queue.
        """

        sqlStr = """
SELECT COUNT(*) FROM %s WHERE dest= :procid
""" % (args['tableName'])

        result = self.execute(sqlStr, args['sqlArgs'])
        result = self.formatOne(result)
        return result[0]

    def moveMsgFromBufferIn(self, args):
        """
        __moveMsg__

        Moves message from one table to another.

        """
        myThread = threading.currentThread()
        sqlStr1 = """
INSERT INTO %s(type,source,dest,payload,time,delay) 
SELECT type,source,dest,payload,time,delay FROM %s order by messageid limit %s
""" % (str(args['target']), str(args['source']), str(args['limit']))
        sqlStr2 = """ 
DELETE FROM %s order by messageid limit %s
""" % (str(args['source']), str(args['limit']))

        myThread.transaction.begin()
        self.execute(sqlStr1+';'+sqlStr2+';commit', {})
        myThread.transaction.commit()

    def moveMsgToBufferOut(self, args):
        """
        _moveMsgToBufferOut_
  
        Moves messages from buffer in or the main queue to buffer out

        """
        # this transaction happens allone.
        myThread = threading.currentThread()

        sqlStr1 = """
INSERT INTO %s(type,source,dest,payload,delay,time) 
SELECT type,source,dest,payload,delay,time FROM %s 
WHERE dest=:procid AND ADDTIME(%s.time,%s.delay) <= CURRENT_TIMESTAMP
ORDER BY messageid LIMIT %s 
""" % (args['target'], args['source'], args['source'], \
        args['source'], args['buffer_size'])

        sqlStr2 = """
DELETE FROM %s WHERE dest=:procid  
AND ADDTIME(%s.time,%s.delay) <= CURRENT_TIMESTAMP 
ORDER BY messageid LIMIT %s 
""" % (args['source'], args['source'], args['source'], args['buffer_size'])  

        myThread.transaction.begin()
        self.execute(sqlStr1+';'+sqlStr2+';commit', {'procid':args['procid']})
        myThread.transaction.commit()


    def removeMessageType(self, args):
        """
        __removeMessageType__

        Removes all messages of a certain type send to a certain destination
        from a message table.
        """

        sqlStr = """
DELETE FROM %s WHERE type=:typeid AND dest=:procid 
""" % (args['tablename'])
        self.execute(sqlStr, args['sqlArgs'])

    def maxId(self, args):
        """
        __maxId__
  
        Determines the max id for the messages in a table.
        This is used for purging the history while keeping the tail of 
        history.
        """
 
        sqlStr = """
SELECT MAX(messageid) from %s
""" % (args['table'])
        result = self.execute(sqlStr, {})
        return self.formatOne(result)

    def purgeHistory(self, args):
        """
        __purgeHistory__

        Purges a history of messages with an id
        smaller than a certain value.
        """

        sqlStr = """
DELETE FROM %s WHERE messageid < %s
""" % (args['table'], str(args['maxId']))
        self.execute(sqlStr, {})

    def setBufferState(self, args):
        """
        __setBufferState__

        Sets the state of a buffer
        """
        sqlStr = """
INSERT IGNORE INTO ms_check_buffer(buffer, status) VALUES(:buffername,:state)
"""
        self.execute(sqlStr,args)

    def getBufferState(self, args):
        """
        __bufferState__

        Returns the state of the buffer using 
        a blocking (FOR UPDATE) select
        """
        if len(args) == 0:
            return []
        if len(args) == 1:
            sqlStr = """
SELECT buffer, status FROM ms_check_buffer WHERE buffer='%s' FOR UPDATE """ %(str(args[0]))
        else:
            sqlStr = """
SELECT buffer, status FROM ms_check_buffer WHERE buffer IN %s FOR UPDATE """ %(str(tuple(args)))

        result = self.execute(sqlStr,{})
        return self.format(result)

    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args) 
