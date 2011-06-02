#!/usr/bin/env python
# encoding: utf-8

"""
Created by Dave Evans on 2011-04-27.
Copyright (c) 2011 Fermilab. All rights reserved.

"""

from WMCore.Database.CMSCouch import Document, Database


class CouchSink(object):
    """
    Alert sink for pushing alerts to a couch database.
    
    """     
    def __init__(self, config):
        self.config = config
        self.database = Database(self.config.database, self.config.url)
        
        
    def send(self, alerts):
        """
        Handle list of alerts.
        
        """
        retVals = []
        for a in alerts:
            doc = Document(None, a)
            retVal = self.database.commitOne(doc)
            retVals.append(retVal)
        return retVals