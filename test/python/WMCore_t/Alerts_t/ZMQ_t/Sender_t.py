#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-03-14.
Copyright (c) 2011 Fermilab. All rights reserved.

"""


import time
import unittest
from  multiprocessing import Process, Queue

import zmq

from WMCore.Alerts.Alert import Alert, RegisterMsg, UnregisterMsg, ShutdownMsg
from WMCore.Alerts.ZMQ.Sender import Sender



def worker(addr, control, outputQ):
    """
    Worker process needs to simulate Receiver instances,
    that is to have two channels. One work channel (workChannel)
    and controlChannel for control messages.
    
    """
    context = zmq.Context()
    workChannel = context.socket(zmq.PULL)
    workChannel.bind(addr)
    controlChannel = context.socket(zmq.SUB)
    controlChannel.bind(control)
    controlChannel.setsockopt(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(controlChannel, zmq.POLLIN)
    poller.register(workChannel, zmq.POLLIN)
    canFinish = False
    while True:
        socks = dict(poller.poll(timeout = 200))
        if not socks and canFinish:
            break
        if socks.get(workChannel) == zmq.POLLIN:
            msg = workChannel.recv_json()
            outputQ.put(msg)
        if socks.get(controlChannel) == zmq.POLLIN:
            msg = controlChannel.recv_json()
            outputQ.put(msg)
            if msg.has_key(ShutdownMsg.key):
                canFinish = True
    print "Receiver worker finished."
    return 0
        


class SenderTest(unittest.TestCase):
    def setUp(self):
        """
        Start up a dummy receiver in a bg process with a queue to
        receive messages.
        
        """
        self.addr = "tcp://127.0.0.1:5557"
        self.control = "tcp://127.0.0.1:5559"
        self.q = Queue()
        self.p = Process(target = worker, args = (self.addr, self.control, self.q))
        self.p.start()
        
        
    def tearDown(self):
        """
        Clean up.
         
        """
        if self.p.exitcode == None:
            print "Process still runs, although it should not"
            self.p.terminate()
        

    def testSenderBasic(self):
        """
        Test instantiating sender.
        
        """
        s = Sender(self.addr, "Sender_t", self.control)
        s.register()
        for i in range(0, 10):
            # actual alert message sending
            a = Alert(Level = i, Type = "Alert")
            s(a)
        s.unregister()
        
        # now should wait until all messages are processed by the receiving
        # worker - rather than sleep, be deterministic and test also Shutdown
        # control message and and let the worker finish on its own,
        # just wait for it. 
        s.sendShutdown()
        while self.p.exitcode == None:
            print "Waiting for worker (Receiver) process to finish ..."
            time.sleep(0.2)
        
        registered = False
        unregistered = False
        shutdown = False
        msgCount = 0
        # 10 alerts + register + unregister + shutdown means we should get 13
        # messages in the queue
        for i in range(0, 13):
            msg = self.q.get()
            if msg.has_key(u"Register"):
                self.assertEqual(msg[RegisterMsg.key], u"Sender_t")
                registered = True
            if msg.has_key(u"Unregister"):
                self.assertEqual(msg[UnregisterMsg.key], u"Sender_t")
                unregistered = True
            if msg.has_key(u"Shutdown"):
                self.assertEqual(msg[ShutdownMsg.key], True)
                shutdown = True
            if u"Alert" in msg.values():
                msgCount += 1
        
        # 10 alerts, and register, unregister, shutdown messages received
        self.assertEqual(msgCount, 10)
        self.failUnless(registered)
        self.failUnless(unregistered)
        self.failUnless(shutdown)
        
        
    
if __name__ == "__main__":
    unittest.main()