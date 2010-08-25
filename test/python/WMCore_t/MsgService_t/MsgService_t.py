#!/usr/bin/env python
"""
_MsgService_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: MsgService_t.py,v 1.8 2009/07/17 15:58:21 sfoulkes Exp $"
__version__ = "$Revision: 1.8 $"

import commands
import unittest
import logging
import os
import threading
import time

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

from WMCore.WMFactory import WMFactory

from WMQuality.TestInit import TestInit

class MsgServiceTest(unittest.TestCase):
    """
    _MsgService_t_
    
    Unit tests for message services: subscription, priority subscription, buffers,
    etc..
    
    """

    _setup = False
    _teardown = False
    # max number of messages for initial tests.
    _maxMsg = 10
    # buffersize used by message service to test message moving.
    _bufferSize = _maxMsg-2

    # minimum number of messages that need to be in queue
    _minMsg = 20
    # number of publish and gets from queue
    _publishAndGet = 10

    def setUp(self):
        "make a logger instance and create tables"
       
        if not MsgServiceTest._setup:
            self.testInit = TestInit(__file__, os.getenv("DIALECT"))
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema()
            MsgServiceTest._setup = True

    def tearDown(self):
        """
        Deletion of the databases 
        """
        #myThread = threading.currentThread()
        #if MsgServiceTest._teardown :
        #    # call the script we use for cleaning:
        #    self.testInit.clearDatabase()
        #MsgServiceTest._teardown = False

               
    def testA(self):
        """
        __testSubscribe__

        Test subscription of a component.
        """
        myThread = threading.currentThread()
        factory = WMFactory("msgService", "WMCore.MsgService."+ \
            myThread.dialect)

        myThread.transaction = Transaction(myThread.dbi)
        msgService1 = factory.loadObject("MsgService")
        msgService2 = factory.loadObject("MsgService")
        msgService1.registerAs("TestComponent1")
        msgService2.registerAs("TestComponent2")
        myThread.transaction.commit()

        myThread.transaction.begin()
        # 2nd registration checks exception in code
        msgService1.registerAs("TestComponent1")
        msgService2.registerAs("TestComponent2")
        myThread.transaction.commit()

        # subscribe
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
        # check if subscription succeeded
        assert subscriptions == [['Message4TestComponent1',], \
            ['Message4TestComponent2',], \
            ['Message4TestComponent3',], \
            ['Message4TestComponent4',]]
        myThread.transaction.commit()

        # subscribe to priority
        myThread.transaction.begin()
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent1")
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent2")
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent3")
        msgService1.prioritySubscribeTo("PriorityMessage4TestComponent4")
        myThread.transaction.commit()

        # check if subscription succeeded
        myThread.transaction.begin()
        subscriptions = msgService1.prioritySubscriptions()
        assert subscriptions == [['PriorityMessage4TestComponent1',], \
            ['PriorityMessage4TestComponent2',], \
            ['PriorityMessage4TestComponent3',], \
            ['PriorityMessage4TestComponent4',]]
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
        for i in xrange(0, 3*MsgServiceTest._maxMsg):
            msg = {'name' : 'Message4TestComponent1', \
                   'payload' : 'aPayload_'+str(i)}
            msgService1.publish(msg)
        # these message have an instant field and will be delivered right away
        print('Inserting '+str(MsgServiceTest._maxMsg*3*3)+ \
              ' Messages (single insert)')
        start = time.time()
        for i in xrange(0, 3*MsgServiceTest._maxMsg):
            msg = {'name' : 'Message4TestComponent1', \
                   'payload' : 'aPayload_'+str(i), 'instant':True}
            msgService1.publish(msg)
        for i in xrange(0, 3*MsgServiceTest._maxMsg):
            msg = {'name' : 'PriorityMessage4TestComponent1', \
                   'payload' : 'aPayload_'+str(i), 'instant':True}
            msgService1.publish(msg)
        # do some calulcations on insert time
        totalInsert = MsgServiceTest._maxMsg*3*3
        stop = time.time()
        interval = float(float(stop) - float(start) )
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Inserting took: '+str(timePerMessage)+ ' seconds per message')
        # check that the messages indeed have been delivered
        assert msgService1.pendingMsgs() == MsgServiceTest._maxMsg*3*3
        # messages without instant key have not been send yet 
        # as finsihed method has not been called
        myThread.transaction.commit()

        # finish transaction, this will add an additional set of messages
        print('Inserting '+str(MsgServiceTest._maxMsg*2*3)+ \
              ' Messages (bulk insert)')
        start = time.time()
        # here you actually deliver just before a commit.
        msgService1.finish()
        stop = time.time()
        interval = float(float(stop) - float(start) )
        totalInsert = MsgServiceTest._maxMsg*3*2
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Inserting took: '+str(timePerMessage)+ ' seconds per message')

        myThread.transaction.begin()
        pendingMsgs = msgService1.pendingMsgs()
        # check if also other messages where delivered.
        assert msgService1.pendingMsgs() == MsgServiceTest._maxMsg*3*5
        myThread.transaction.commit()

    def testC(self):
        """
        __testC__

        """
        myThread = threading.currentThread()
        myThread.transaction.begin()
        msgService1 = \
            myThread.factory['msgService'].loadObject("MsgService")
        # test use illegal name for component
        for word in  ['ms_message', 'ms_priority_message', \
                      'buffer_out', 'buffer_in']:
            exReached = True
            try:
                msgService1.registerAs(word)
                exReached = False
            except Exception,ex:
                logging.debug("Passed: "+str(ex))
                pass
            assert exReached

        msgService1.registerAs("TestComponent1")
        myThread.transaction.commit()

        myThread.transaction.begin()
        msgService1.remove("Message4TestComponent1")
        msgService1.remove("Message4TestComponentNotSubscribedTo")
        myThread.transaction.commit()

        myThread.transaction.begin()
        # remove all messages
        msgService1.purgeMessages()
        assert msgService1.pendingMsgs() == 0
        myThread.transaction.commit()
        
        # test publish unique.
        myThread.transaction.begin()
        for i in xrange(0, 3*MsgServiceTest._maxMsg):
            msg = {'name' : 'Message4TestComponent1', \
                   'payload' : 'aPayload_'+str(i), 'instant':True}
            msgService1.publishUnique(msg)
        for i in xrange(0, 3*MsgServiceTest._maxMsg):
            msg = {'name' : 'PriorityMessage4TestComponent1', \
                   'payload' : 'aPayload_'+str(i), 'instant':True}
            msgService1.publishUnique(msg)
        myThread.transaction.commit()
        myThread.transaction.begin()
        # as we published unique only 3 entries where made 
        # (1 messages is subscribed by two copmonents)
        assert msgService1.pendingMsgs() == 3
        myThread.transaction.commit()

    def testD(self):
        """
        __testD__
   
        Inserts a lot of messages to multiple subscribers
        and then retrieves them.
        """
        
        # do some insert and get tests and measure it.
        myThread = threading.currentThread()
        myThread.transaction.begin()
        msgServiceL = []
        for i in xrange(0, 10):
            msgService = \
                myThread.factory['msgService'].loadObject("MsgService")
            msgService.registerAs("TestComponent"+str(i))
            msgService.subscribeTo("msg_for_"+str(i))
            msgService.subscribeTo("msg_for_all")
            msgService.prioritySubscribeTo("priority_msg_for_"+str(i))
            msgService.prioritySubscribeTo("priority_msg_for_all")
            msgServiceL.append(msgService)
        msgServiceL[0].purgeMessages()
        myThread.transaction.commit()

        # first we insert a lot of messages by one service
        print('Inserting: '+str(2*MsgServiceTest._minMsg*10) +' messages')
        start = time.time()
        for i in xrange(0, MsgServiceTest._minMsg):
            msg = {'name':'msg_for_all', \
                   'payload':'from0_normal_'+str(i)}
            myThread.transaction.begin()
            msgServiceL[0].publish(msg)
            msg = {'name':'priority_msg_for_all', \
                   'payload':'from0_priority_'+str(i)}
            msgServiceL[0].publish(msg)
            myThread.transaction.commit()
            msgServiceL[0].finish()
        stop = time.time()
        interval = float(float(stop) - float(start) )
        totalInsert = 2*MsgServiceTest._minMsg*10
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Inserting took: '+str(timePerMessage)+ ' seconds per message')

        print('Retrieving: '+str(2*MsgServiceTest._minMsg*10) +' messages')
        start = time.time()
        for j in xrange(0, MsgServiceTest._minMsg):
            for i in xrange(0, 10):
                msg = msgServiceL[i].get() 
                assert msg['payload'] == 'from0_priority_'+str(j)
                # we need to finish the message handling.
                msgServiceL[i].finish()

        # once we have them all we should get the normal messages
        for j in xrange(0, MsgServiceTest._minMsg):
            for i in xrange(0, 10):
                msg = msgServiceL[i].get() 
                assert msg['payload'] == 'from0_normal_'+str(j)
                # we need to finish the message handling.
                msgServiceL[i].finish()
        stop = time.time()
        interval = float(float(stop) - float(start) )
        totalInsert = 2*MsgServiceTest._minMsg*10
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Retrieving took: '+str(timePerMessage)+ ' seconds per message')

        myThread.transaction.begin()
        # we should now have 0 messages left.
        assert msgServiceL[0].pendingMsgs() == 0
        myThread.transaction.commit()
        # purge everything.
        
    def testE(self):
        """
        __testE__

        Tests the insert/delete time in a large message queue. It firs inserts
        a lot of messages in the queue after which it inserts/deletes messages several times
        and tests the time it takes to do that.
        """

        MsgServiceTest._teardown = True
        # do some insert and get tests and measure it.
        myThread = threading.currentThread()
        myThread.transaction.begin()
        msgServiceL = []
        for i in xrange(0, 10):
            msgService = \
                myThread.factory['msgService'].loadObject("MsgService")
            msgService.registerAs("TestComponent"+str(i))
            msgService.subscribeTo("msg_for_"+str(i))
            msgService.subscribeTo("msg_for_all")
            msgService.prioritySubscribeTo("priority_msg_for_"+str(i))
            msgService.prioritySubscribeTo("priority_msg_for_all")
            msgServiceL.append(msgService)
        msgServiceL[0].purgeMessages()
        myThread.transaction.commit()

        # first we insert a lot of messages by one service
        print('Inserting: '+str(2*MsgServiceTest._minMsg*10) +' messages')
        start = time.time()
        for i in xrange(0, MsgServiceTest._minMsg):
            msg = {'name':'msg_for_all', \
                   'payload':'from0_normal_'+str(i)}
            myThread.transaction.begin()
            msgServiceL[0].publish(msg)
            msg = {'name':'priority_msg_for_all', \
                   'payload':'from0_priority_'+str(i)}
            msgServiceL[0].publish(msg)
            myThread.transaction.commit()
            msgServiceL[0].finish()
        stop = time.time()
        interval = float(float(stop) - float(start) )
        totalInsert = 2*MsgServiceTest._minMsg*10
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Inserting took: '+str(timePerMessage)+ ' seconds per message')

        print('Retrieving: '+ str(2*MsgServiceTest._minMsg*10) + \
            ' messages (but everytime we insert one too)')
        # then we do alternate insert / get to check the amount of time needed.
        # first we should get the priority messages 
        start = time.time()
        for j in xrange(0, MsgServiceTest._minMsg):
            myThread.transaction.begin()
            msg = {'name':'msg_for_all', 'payload':'from0_normal_added'}
            msgServiceL[0].publish(msg)
            myThread.transaction.commit()
            msgServiceL[0].finish()
            for i in xrange(0, 10):
                msg = msgServiceL[i].get() 
                assert msg['payload'] == 'from0_priority_'+str(j)
                # we need to finish the message handling.
                msgServiceL[i].finish()

        # once we have them all we should get the normal messages
        for j in xrange(0, MsgServiceTest._minMsg):
            myThread.transaction.begin()
            msg = {'name':'msg_for_all', 'payload':'from0_normal_added'}
            msgServiceL[0].publish(msg)
            myThread.transaction.commit()
            msgServiceL[0].finish()
            for i in xrange(0, 10):
                msg = msgServiceL[i].get() 
                assert msg['payload'] == 'from0_normal_'+str(j)
                # we need to finish the message handling.
                msgServiceL[i].finish()
        stop = time.time()
        interval = float(float(stop) - float(start) )
        totalInsert = 2*MsgServiceTest._minMsg*10
        timePerMessage = float ( float(interval) / float(totalInsert) )
        print('Insert/Retrieving took: '+str(timePerMessage)+ \
            ' seconds per insert/retrieve message on queue with '+ \
            str(totalInsert)+' messages')

        myThread.transaction.begin()
        # we should now have 0 messages left.
        assert msgServiceL[0].pendingMsgs() == 2*MsgServiceTest._minMsg*10
        myThread.transaction.commit()
        # purge everything.

    def runTest(self):
        self.testA()
        self.testB()
        self.testC()
        self.testD()
        self.testE()

if __name__ == "__main__":
    unittest.main()
