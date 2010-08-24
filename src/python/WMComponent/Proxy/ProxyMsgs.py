#!/usr/bin/env python

"""
A temporary interface to the proxy message queue. This is a stripped
down version of the old message service in the prodagent that
is compliant with the old schema.
"""

__revision__ = "$Id: ProxyMsgs.py,v 1.3 2008/09/29 16:10:56 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "fvlingen@caltech.edu"




import time
import os
import socket
import logging

# we use this as we want to make a 'custom' connection
# to the other database through the old msg service interface.
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

from WMCore.Database.DBFormatter import DBFormatter

class ProxyMsgs:
    """
    _ProxyMsg_
    """
    
    def __init__(self, contactinfo):
        """
        __init__
        
        """
        self.contactinfo = contactinfo

        options = {}
        options['unix_socket'] = os.getenv("DBSOCK")
        dbFactory = DBFactory(logging.getLogger(), contactinfo, options)

        self.dbi = dbFactory.connect()
        self.trans = Transaction(self.dbi)
        self.trans.commit()
        self.dbformat = DBFormatter(logging.getLogger(), self.dbi)

        # initialize internal variables
        self.name = None
        self.procid = None

        # parameters
        self.refreshPeriod = 60 * 60 * 12
        self.pollTime = 5

    def registerAs(self, name):
        """
        __registerAs__
        """
        
        # set component name
        self.name = name
        
        # get process data
        currentPid = os.getpid()
        currentHost = socket.gethostname()
        
        # open connection
                                                                                
        # check if process name is in database
        sqlCommand = """
                     SELECT procid, host, pid
                       FROM ms_process
                       WHERE name = '""" + name + """'
                     """
        self.trans.begin()
        result = self.trans.processData(sqlCommand, {})
        rows = self.dbformat.format(result)
        self.trans.commit()

        # process was registered before
        if len(rows) == 1:
            # get data
            procid, host, pid = rows[0]
            
            # if pid and host are the same, get id and return
            if host == currentHost and pid == currentPid:
                self.procid = procid
                return
            
            # process was replaced, update info
            else:
                sqlCommand = """
                         UPDATE
                             ms_process
                           SET
                             pid = '"""+ str(currentPid) + """',
                             host = '"""+ currentHost + """'
                           WHERE name = '""" + name + """'
                         """
                self.trans.begin()
                self.trans.processData(sqlCommand, {})
                self.trans.commit()
                self.procid = procid
                return
                
        # register new process in database
        sqlCommand = """
                     INSERT
                       INTO ms_process
                         (name, host, pid)
                       VALUES
                         ('""" + name + """',
                         '""" + currentHost + """',
                         '""" + str(currentPid) + """')
                     """
        self.trans.begin()
        self.trans.processData(sqlCommand, {})

        # get id
        sqlCommand = "SELECT LAST_INSERT_ID()"
        result = self.trans.processData(sqlCommand)
        row = self.dbformat.formatOne(result)
        self.procid = row[0]
        self.trans.commit()

    def subscribeTo(self, name):
        """
        __subscribeTo__
        """

        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
        self.trans.begin()
        result = self.trans.processData(sqlCommand, {})
        rows = self.dbformat.format(result)

        # get message type id
        if len(rows) == 1:
            
            # message type was registered before, get id
            typeid = rows[0][0]
            
        else:

            # not registered before, so register now
            sqlCommand = """
                         INSERT
                           INTO ms_type
                             (name)
                           VALUES
                             ('""" + name + """')
                         """
            self.trans.processData(sqlCommand, {})
            
            # get id
            sqlCommand = "SELECT LAST_INSERT_ID()"
            result = self.trans.processData(sqlCommand, {})
            row = self.dbformat.formatOne(result)
            typeid = row[0]

        # check if there is an entry in subscription table
        sqlCommand = """
                     SELECT procid, typeid
                       FROM ms_subscription
                       WHERE procid = '""" + str(self.procid) + """'
                         AND typeid = '""" + str(typeid) + """'
                     """
        result = self.trans.processData(sqlCommand, {})
        rows = self.dbformat.format(result)

        # entry registered before, just return
        if len(rows) == 1:
            self.trans.commit()
            return
        
        # not registered, do it now
        sqlCommand = """
                     INSERT
                       INTO ms_subscription
                         (procid, typeid)
                       VALUES ('""" + str(self.procid) + """',
                               '""" + str(typeid) + """')
                     """
        self.trans.processData(sqlCommand, {})

        # return
        self.trans.commit()

    def publish(self, name, payload, delay="00:00:00"):
        """
        _publish_
        """
        
        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
        self.trans.begin()                     
        result = self.trans.processData(sqlCommand, {})

        rows = self.dbformat.format(result)

        # get message type id
        if len(rows) == 1:
            
            # message type was registered before, get id
            typeid = rows[0][0]
            
        else:

            # not registered before, so register now
            sqlCommand = """
                         INSERT
                           INTO ms_type
                             (name)
                           VALUES
                             ('""" + name + """')
                         """
            self.trans.processData(sqlCommand, {})

            # get id
            sqlCommand = "SELECT LAST_INSERT_ID()"
            result = self.trans.processData(sqlCommand, {})
            row = self.dbformat.formatOne(result)
            typeid = row[0]
            
        # get destinations
        sqlCommand = """
                     SELECT procid
                       FROM ms_subscription
                       WHERE typeid = '""" + str(typeid) + """'
                     """
        result = self.trans.processData(sqlCommand, {})
        dests = self.dbformat.format(result)
        
        destinations = self.getList(dests)
        
        # add message to database for delivery
        destCount = 0
        for dest in destinations:
            sqlCommand = """
                         INSERT
                           INTO ms_message
                             (type, source, dest, payload,delay)
                           VALUES ('""" + str(typeid) + """',
                                   '""" + str(self.procid) + """',
                                   '""" + str(dest) + """',
                                   '""" + payload + """',
                                   '""" + str(delay)+ """')
                         """
            self.trans.processData(sqlCommand)
            sqlCommand = """
                         INSERT
                           INTO ms_history
                             (type, source, dest, payload,delay)
                           VALUES ('""" + str(typeid) + """',
                                   '""" + str(self.procid) + """',
                                   '""" + str(dest) + """',
                                   '""" + payload + """',
                                   '""" + str(delay)+ """')
                         """
            self.trans.processData(sqlCommand)
            destCount += 1

        # return
        self.trans.commit()
        return destCount
    
    
    def publishUnique(self, name, payload, delay="00:00:00"):
        """
          _publishUnique_
        """
        
        # check if message type is in database
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + name + """'
                     """
        self.trans.begin()
        results = self.trans.processData(sqlCommand, {})
        self.trans.commit()
 
        rows = self.dbformat.format(results)

        if len(rows) == 0:
            # not registered before, so cant have any instances
            return self.publish(name, payload, delay)
        
        # message type was registered before, get id
        typeid = rows[0]
                        
        # message known - how many in queue?
        sqlCommand = """
                     SELECT COUNT(*)
                       FROM ms_message
                       WHERE type = '""" + str(typeid) + """'
                     """
    
        self.trans.begin()
        result = self.trans.processData(sqlCommand)
        self.trans.commit()
        
        num = self.dbformat.format(result)[0]
        
        if num == 0:
            # no messages - so publish
            return self.publish(name, payload, delay)
        
        # message exists - do not publish another
        return 0
    
    def get(self, wait = True):
        """
        __get__
        """


        # get messages command
        sqlCommand = """
                     SELECT messageid, name, payload
                       FROM ms_message, ms_type
                       WHERE
                         typeid=type and
                         ADDTIME(time,delay) <= CURRENT_TIMESTAMP and
                         dest='""" + str(self.procid) + """'
                       ORDER BY time,messageid
                       LIMIT 1
                       
                     """

        # check for messages
        rows = []
        while True:

            # get messsages
            result = 0
            # execute command
            self.trans.begin()
            result = self.trans.processData(sqlCommand, {})
            self.trans.commit()
            rows = self.dbformat.format(result)
            # there is one, return it
            if len(rows) == 1:
                break

            # no messages yet
            if not wait:

                # return immediately with no message
                return (None, None)

            # or wait and try again after some time
            time.sleep(self.pollTime)      
 
        # get data
        messageid, type, payload = rows[0]
        
        # remove messsage
        sqlCommand = """
                     DELETE 
                       FROM ms_message
                       WHERE
                         messageid='""" + str(messageid) + """'
                       LIMIT 1
                     """
        self.trans.begin()
        self.trans.processData(sqlCommand, {})
        self.trans.commit()

        return (type, payload)

    def purgeMessages(self):
        """
        __purgeMessages__
        
        """

        # remove all messsages
        sqlCommand = """
                     DELETE 
                       FROM ms_message
                     """
        self.trans.begin()
        self.trans.processData(sqlCommand, {})
        self.trans.commit()

    def remove(self, messageType):
        """
        __remove__

        """

        # get message type (if it is in database)
        sqlCommand = """
                     SELECT typeid
                       FROM ms_type
                       WHERE name = '""" + messageType + """'
                     """
        self.trans.begin()
        result = self.trans.processData(sqlCommand, {})
        rows = self.dbformat.format(result)
        self.trans.commit()

        # no rows, nothing to do
        if len(rows) == 0:
            return

        # get type
        typeid = rows[0]

        # remove all messsages
        sqlCommand = """
                     DELETE
                       FROM ms_message
                      WHERE type='""" + str(typeid) + """'
                        AND dest='""" + str(self.procid) + """'
                     """
        self.trans.begin()
        self.trans.processData(sqlCommand)
        self.trans.commit()

    def cleanHistory(self, hours):
        """
        __cleanHistory__
        """

        timeval = "-%s:00:00" % hours

        # remove all messsages
        sqlCommand = """
                     DELETE 
                       FROM ms_history
                       WHERE
                          time < ADDTIME(CURRENT_TIMESTAMP,'-%s');
                          
                          """ % timeval

        self.trans.begin()
        self.trans.processData(sqlCommand, {})
        self.trans.commit()

    def getList(self, dests):
        """
        __getList__
        
        """
        list = []
        for dest in dests:
            list.append(dest[0])
        return list

