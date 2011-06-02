#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-02-25.
Copyright (c) 2011 Fermilab. All rights reserved.

"""


import os

import zmq

from WMCore.Alerts.Alert import RegisterMsg, UnregisterMsg, ShutdownMsg



class Sender(object):
    """
    ZMQ sender to dispatch alerts to a target.
    
    """
    def __init__(self, target, label = None, controller = "tcp://127.0.0.1:5559"):
        self._label = label or "Sender_%s" % os.getpid()
        context = zmq.Context()
        # set up a channel to send work
        self._workChannel = context.socket(zmq.PUSH)
        self._workChannel.connect(target)
        # set up a control channel
        self._contChannel = context.socket(zmq.PUB)
        self._contChannel.connect(controller)
        
        
    def __call__(self, alert):
        """        
        Send the alert instance to the target that this sender represents.
        
        """    
        self._workChannel.send_json(alert)
        
        
    def register(self):
        """
        Send a register message to the target.
        
        """
        self._contChannel.send_json(RegisterMsg(self._label))
        
        
    def unregister(self):
        """
        Send an unregister message to the target.
        
        """
        self._contChannel.send_json(UnregisterMsg(self._label))
        
        
    def sendShutdown(self):
        """
        Tells the Receiver to shut down. 
        This method mostly here for convenience in tests.
        
        """
        self._contChannel.send_json(ShutdownMsg())