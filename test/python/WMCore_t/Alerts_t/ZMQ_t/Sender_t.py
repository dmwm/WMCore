#!/usr/bin/env python
# encoding: utf-8
"""
Sender_t.py

Created by Dave Evans on 2011-03-14.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
import zmq
import time
from WMCore.Alerts.ZMQ.Receiver import Receiver
from WMCore.Alerts.ZMQ.Sender import Sender

from  multiprocessing import Process, Queue



def worker(addr, control, outputQ):
    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.bind(addr)
    controller = context.socket(zmq.SUB)
    controller.bind(control)
    controller.setsockopt(zmq.SUBSCRIBE, "")
    poller = zmq.Poller()
    poller.register(controller, zmq.POLLIN)
    poller.register(work_receiver, zmq.POLLIN)
    while True:
        socks = dict(poller.poll())
        if socks.get(work_receiver) == zmq.POLLIN:
            outputQ.put(work_receiver.recv_json())
        if socks.get(controller) == zmq.POLLIN:
            outputQ.put(controller.recv_json())

    
    
    

class Sender_t(unittest.TestCase):
    
    
    def setUp(self):
        """
        start up a dummy receiver in a bg process with a queue to receive messages
        """
        self.addr = "tcp://127.0.0.1:5557"
        self.control = "tcp://127.0.0.1:5559"
        self.q = Queue()
        self.p = Process(target=worker, args=(self.addr, self.control, self.q))
        self.p.start()
        
        
        
    def tearDown(self):
        """
        clean up 
        """
        self.p.terminate()          
        

    def testA(self):
        """
        test instantiating sender
        """
        s = Sender(self.addr, "Sender_t", self.control )
        s.register()
        for i in range(0, 10):
            s({"Alert" : "Test", "Level": i})
        s.unregister()
        # need this to give the worker time to finish up
        time.sleep(1)
        registered = False
        unregistered = True
        msg_count = 0
        # 10 alerts + reg + unreg means we should get 12 messages in the queue
        for i in range(0, 12):
            msg = self.q.get()
            if msg.has_key(u'Register'):
                self.assertEqual(msg[u'Register'], u"Sender_t")
                registered = True
            if msg.has_key(u'Unregister'):
                self.assertEqual(msg[u'Unregister'], u"Sender_t")
                unregistered = True
            if msg.has_key(u"Alert"):
                msg_count += 1
        
        # 10 alerts and both register and unregister message received
        self.failUnless(registered)
        self.failUnless(unregistered)
        self.assertEqual(msg_count, 10)
        
        
    
if __name__ == '__main__':
    unittest.main()