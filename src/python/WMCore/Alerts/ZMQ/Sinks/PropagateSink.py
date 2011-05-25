#!/usr/bin/env python
# encoding: utf-8
"""
PropagateSink.py

Created by Dave Evans on 2011-04-29.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
from WMCore.Alerts.ZMQ.Sender import Sender

class PropagateSink(object):
    """
    _PropagateSink_
    
    Alert forwarder to another alert processor
    """        
    def __init__(self, config):
        self.config = config
        self.address = config.address
        self.label = getattr(config, "label", None)
        self.sender = Sender(self.address, self.label)
        
    
    def send(self, alerts):
        """
        _send_
        
        handle list of alerts
        """
        [ self.sender(a) for a in alerts ]


import unittest
import time
from WMCore.Alerts.Alert import Alert
from WMCore.Configuration import ConfigSection



class PropagateSinkTests(unittest.TestCase):
    def setUp(self):
        self.config = ConfigSection('propagate')
        


if __name__ == '__main__':
    unittest.main()