#!/usr/bin/env python
# encoding: utf-8
"""
Reciever.py

Created by Dave Evans on 2011-03-04.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import zmq
import time


class Receiver:
    """
    _Receiver_
    
    Basic message reciever that handles alerts sent from multiple clients
    Clients can also register/unregister via control messages which helps during testing
    
    The class starts up with a target port to bind to for work and optionally for control messages.
    A handler method (callable) is used to handle alert data from the work receiver.
    
    If you want the polling loop to stop after all senders have unsubscribed, set close_when_empty to True
    before calling start and it will shut down when the last sender unregisters.
    
    Example:
    
    def printer(alert):
          print alert

    rec = Receiver("tcp://127.0.0.1:5557", printer)
    rec.close_when_empty = True 
    rec.start()
    
    """
    
    def __init__(self, target, handler, control = "tcp://127.0.0.1:5559"):
        self.context = zmq.Context()
        # receiver pulls in alerts to pass to a handler
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind(target)
        # control messages
        self.controller = self.context.socket(zmq.SUB)
        self.controller.bind(control)
        self.controller.setsockopt(zmq.SUBSCRIBE, "")
        # handler to be called when alert data is passed in
        self.handler = handler
        # keep map of who registered and when
        self.registered_senders = {}
        # flag to control behaviour 
        self.close_when_empty = False

        
    def start(self):
        """
        _start_
        
        Start polling reciever and control channel
        
        """
        poller = zmq.Poller()
        poller.register(self.controller, zmq.POLLIN)
        poller.register(self.receiver, zmq.POLLIN)
        # Loop and accept messages from both channels, acting accordingly
        while True:
            socks = dict(poller.poll())
            if socks.get(self.receiver) == zmq.POLLIN:
                # alert data are sent to the handler
                alert = self.receiver.recv_json()
                self.handler(alert)
            # control messages
            if socks.get(self.controller) == zmq.POLLIN:
                control_message = self.controller.recv_json()
                # direct shutdown command
                if control_message.has_key("Shutdown"):
                    break
                # new sender registers itself
                if control_message.has_key("Register"):
                    senderId = control_message['Register']
                    self.registered_senders[senderId] = time.time()
                    print "Registered %s@%s" % (senderId, self.registered_senders[senderId] )
                # sender unregisters itself
                if control_message.has_key("Unregister"):
                    senderId = control_message['Unregister']
                    if self.registered_senders.has_key(senderId):
                        del self.registered_senders[senderId]
                        print "Unregistered %s@%s" % (senderId, time.time())
                    # if set, check for shutdown condition when all senders have unsubscribed
                    if self.close_when_empty:
                        if len(self.registered_senders.keys()) == 0:
                            break
                            
            

    