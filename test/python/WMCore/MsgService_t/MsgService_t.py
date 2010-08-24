#!/usr/bin/env python
"""
_MsgService_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: MsgService_t.py,v 1.3 2008/08/28 20:40:51 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"

import commands
import unittest
import logging
import os
import threading
import time

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

from WMCore.WMFactory import WMFactory

class MsgServiceTest(unittest.TestCase):
    """
    _MsgService_t_
    
    Unit tests for message services: subscription, priority subscription, buffers,
    etc..
    
    """

    _setup = False
    _teardown = False
    _maxMsg = 100
    _bufferSize = _maxMsg-2

    def setUp(self):
        "make a logger instance and create tables"
       
        if not MsgServiceTest._setup: 
            logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('MsgServiceTest')
            myThread.dialect = 'MySQL'
        
            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("MYSQLDATABASE"), \
                options)
        
            myThread.dbi = dbFactory.connect() 

            factory = WMFactory("msgService", "WMCore.MsgService."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute()
            if createworked:
                logging.debug("MsgService tables created")
            else:
                logging.debug("MsgService tables could not be created, \
                    already exists?")
                                              
            MsgServiceTest._setup = True

    def tearDown(self):
        """
        Delete the databases
        """
        myThread = threading.currentThread()
        if MsgServiceTest._teardown:
            myThread.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root --socket='+os.getenv("DBSOCK")+' drop '+os.getenv("DBNAME")))
            myThread.logger.debug(commands.getstatusoutput('mysqladmin -u root --socket='+os.getenv("DBSOCK")+' create '+os.getenv("DBNAME")))
            myThread.logger.debug("database deleted")
               
               
    def testA(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        msgService1 = \
            myThread.factory['msgService'].loadObject("MsgService")
        msgService2 = \
            myThread.factory['msgService'].loadObject("MsgService")
        msgService1.registerAs("TestComponent1")
        msgService2.registerAs("TestComponent2")
        myThread.transaction.commit()

        myThread.transaction.begin()
        # 2nd registration checks exception in code
        msgService1.registerAs("TestComponent1")
        msgService2.registerAs("TestComponent2")
        myThread.transaction.commit()

        myThread.transaction.begin()
        msgService1.subscribeTo("Message4TestComponent1")
        msgService1.subscribeTo("Message4TestComponent2")
        msgService1.subscribeTo("Message4TestComponent3")
        msgService1.subscribeTo("Message4TestComponent4")
        msgService2.subscribeTo("Message4TestComponent1")
        msgService2.subscribeTo("Message4TestComponent2")
        myThread.transaction.commit()

        myThread.transaction.begin()
        subscriptions = msgService1.subscriptions()
        assert subscriptions == [('Message4TestComponent1',), \
            ('Message4TestComponent2',), \
            ('Message4TestComponent3',), \
            ('Message4TestComponent4',)]
        myThread.transaction.commit()

        myThread.transaction.begin()
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent1")
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent2")
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent3")
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent4")
        myThread.transaction.commit()

        myThread.transaction.begin()
        subscriptions = msgService1.prioritySubscriptions()
        assert subscriptions == [('PriorityMessage4TestComponent1',), \
            ('PriorityMessage4TestComponent2',), \
            ('PriorityMessage4TestComponent3',), \
            ('PriorityMessage4TestComponent4',)]
        myThread.transaction.commit()
        
    def testB(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        msgService1 = \
            myThread.factory['msgService'].loadObject("MsgService")
        msgService2 = \
            myThread.factory['msgService'].loadObject("MsgService")
        msgService1.registerAs("TestComponent1")
        msgService1.buffer_size = MsgServiceTest._bufferSize
        msgService2.registerAs("TestComponent2")
        myThread.transaction.commit()
        # create some messages.
        myThread.transaction.begin()
        for i in xrange(0,3*MsgServiceTest._maxMsg):
            msg = {'name' : 'Message4TestComponent1', 'payload' : 'aPayload_'+str(i)}
            msgService1.publish(msg)
        # these message have an instant field and will be delivered
        print('Inserting '+str(MsgServiceTest._maxMsg*3*3)+' Messages (single insert)')
        start = time.time()
        for i in xrange(0,3*MsgServiceTest._maxMsg):
            msg = {'name' : 'Message4TestComponent1', 'payload' : 'aPayload_'+str(i), 'instant':True}
            msgService1.publish(msg)
        for i in xrange(0,3*MsgServiceTest._maxMsg):
            msg = {'name' : 'PriorityMessage4TestComponent1', 'payload' : 'aPayload_'+str(i), 'instant':True}
            msgService1.publish(msg)
        totalInsert = MsgServiceTest._maxMsg*3*3
        stop = time.time()
        interval = float(float(stop) - float(start) )
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Inserting took: '+str(timePerMessage)+ ' seconds per message')
        assert msgService1.pendingMsgs() == MsgServiceTest._maxMsg*3*3
        # messages without instant key have not been send yet 
        # as finsihed method has not been called
        myThread.transaction.commit()
        myThread.transaction.begin()

        # finish transaction, this will add an additional set of messages
        print('Inserting '+str(MsgServiceTest._maxMsg*2*3)+' Messages (bulk insert)')
        start = time.time()
        msgService1.finish()
        stop = time.time()
        interval = float(float(stop) - float(start) )
        totalInsert = MsgServiceTest._maxMsg*3*2
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Inserting took: '+str(timePerMessage)+ ' seconds per message')
        myThread.transaction.commit()
        myThread.transaction.begin()
        pendingMsgs = msgService1.pendingMsgs()
        assert msgService1.pendingMsgs() == MsgServiceTest._maxMsg*3*5
        myThread.transaction.commit()

if __name__ == "__main__":
    unittest.main()
