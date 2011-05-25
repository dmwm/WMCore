#!/usr/bin/env python
# encoding: utf-8
"""
CouchSink.py

Created by Dave Evans on 2011-04-27.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
from WMCore.Database.CMSCouch import Document, Database


class CouchSink:
    """
    _CouchSink_
    
    Alert sink for pushing alerts to a couch database
    """     
    def __init__(self, config):
        self.config = config
        self.database = Database(self.config.database, self.config.url)
        
    def send(self, alerts):
        """
        _send_
        
        Handle list of alerts
        """
        doc = Document(None, alerts)
        self.database.commitOne(doc)
        
        


