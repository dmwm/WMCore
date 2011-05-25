#!/usr/bin/env python
# encoding: utf-8
"""
FileSink.py

Created by Dave Evans on 2011-04-28.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
from collections import deque
import json

class FileSink:
    """
    _FileSink_
    
    Coroutine like sink for flushing alerts to a JSON file.
    Uses a deque to rotate entries
    Potentially a lot of file churn on this if fed alerts one at a time, may be worth 
    doing a Buffered version that flushes to file every N alerts instead, but this QND job 
    will do for now
    
    """
    def __init__(self, config):
        self.outputfile = config.outputfile
        self.depth = getattr(config, "depth", 100)
        self.deque = None


    def load(self):
        """
        _load_
        
        Load the current set of alerts in the file into the deque
        """
        if self.deque == None:
            self.deque = deque(maxlen = self.depth)
        else:
            handle = open(self.outputfile, 'r')
            self.deque.extend(list(json.load(handle)))
            handle.close()
        
    def save(self):
        """
        _save_
        
        persist the deque to the file
        """
        handle = open(self.outputfile, 'w')
        json.dump(list(self.deque), handle)
        handle.close()
        self.deque.clear()
        
    
    def send(self, alerts):
        """
        _send_
        Generator like interface
        """
        self.load()
        self.deque.extend(alerts)
        self.save()




