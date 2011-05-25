#!/usr/bin/env python
# encoding: utf-8
"""
Receiver_t.py

Created by Dave Evans on 2011-03-14.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import unittest
import zmq
import time
import sys
from multiprocessing import Process, Queue
from WMCore.Alerts.ZMQ.Receiver import Receiver


def simpleWorker(addr, ctrl):
    """
    _simpleWorker_
    
    Start a sender that pauses and then shuts down the receiver
    """
    time.sleep(1)
    context = zmq.Context()
    # Set up a channel to send work
    sender = context.socket(zmq.PUSH)
    sender.connect(addr)
    
    controller = context.socket(zmq.PUB)
    controller.connect(ctrl)
    
    controller.send_json({"Register" : "Receiver_t" })
    time.sleep(1)
    controller.send_json({"Unregister" : "Receiver_t" })    
    time.sleep(1)
    controller.send_json({"Shutdown" : True})

def worker(addr, ctrl, outputQ, nAlerts):
    """
    _worker_
    
    Util to start a sender and send some alert messages to the main thread reciever
    """
    time.sleep(1)
    context = zmq.Context()
    # Set up a channel to send work
    sender = context.socket(zmq.PUSH)
    sender.connect(addr)
    
    controller = context.socket(zmq.PUB)
    controller.connect(ctrl)
    
    controller.send_json({"Register" : "Receiver_t" })
    for i in range(0, nAlerts):
        time.sleep(1)
        sender.send_json({"Alert" : "Test", "Level": i})
    time.sleep(1)
    controller.send_json({"Unregister" : "Receiver_t" })
    
    controller.send_json({"Shutdown" : True})
    
class Receiver_t(unittest.TestCase):
    """
    Test Case for Receiver 
    """
    
    
    def setUp(self):
        """ start bg process that sends messages"""
        self.addr = "tcp://127.0.0.1:5557"
        self.ctrl = "tcp://127.0.0.1:5559"
        
        
    def tearDown(self):
        #self.p.terminate()
        pass
        
    def testA(self):
        
        self.p = Process(target=simpleWorker, args=(self.addr, self.ctrl))
        self.p.start()
        
        
        printer = lambda x : sys.stdout.write(str(x))
        rec = Receiver(self.addr, printer, self.ctrl)
        rec.start()
        
    def testB(self):
        """
        test sending alerts that are buffered into a queue
        """
        q = Queue()
        #start a process that generates 5 alerts 
        self.p = Process(target=worker, args=(self.addr, self.ctrl, q, 5))
        self.p.start()
        
        # alerts recieved will be added to the queue
        handler = lambda x: q.put(x)
        rec = Receiver(self.addr, handler, self.ctrl)
        rec.start()
        
        # worker will send shutdown message and execution will resume here
        # check the content of the queue is 5 alert messages
        alert_count = 0
        while not q.empty():
            alert = q.get()
            self.failUnless(alert.has_key("Alert"))
            alert_count += 1
        self.assertEqual(alert_count)
        
    
    
if __name__ == '__main__':
    unittest.main()