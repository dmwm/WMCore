#!/usr/bin/env python
# encoding: utf-8
"""
Sender.py

Created by Dave Evans on 2011-02-25.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import zmq



class Sender:
    """
    ØMQ sender to dispatch alerts to a target
    """
    def __init__(self, target, label = None, controller = "tcp://127.0.0.1:5559"):
        self.target = target
        self.label = label or "Sender_%s" % os.getpid()
        self.context = zmq.Context()
        # Set up a channel to send work
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect(target)
        
        self.controller = self.context.socket(zmq.PUB)
        self.controller.connect(controller)
        
        
    def __call__(self, alert):
        """
        _operator(alert)_
        
        Send the alert instance to the target that this sender represents
        """    
        self.sender.send_json(dict(alert))
        
    def register(self):
        """
        _register_
        
        Send a register message to the target
        
        """
        self.controller.send_json({"Register" : self.label })
        
    def unregister(self):
        """
        _unregister_
        
        Send an unregister message to the target
        """
        self.controller.send_json({"Unregister" : self.label })
        
    def send_shutdown(self):
        """
        _send_shutdown_
        
        Tells the Receiver to shut down. 
        This method mostly here for convienience in tests
        """
        self.controller.send_json({"Shutdown" : True })
        

        