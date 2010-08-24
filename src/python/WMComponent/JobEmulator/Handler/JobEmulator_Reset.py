#!/usr/bin/env python

"""
Auto generated stub be careful with editing,
Inheritance is preferred.
"""

import os
import threading

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.WMFactory import WMFactory

class JobEmulator_Reset(BaseHandler):

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        self.factory = WMFactory('factory','')
        myThread = threading.currentThread()
        self.queries = self.factory.loadObject('WMComponent.JobEmulator.Database.'+myThread.dialect+'.Sites')
        self.placeboTrackerDb = self.factory.loadObject('WMComponent.JobEmulator.Database.'+myThread.dialect+'.PlaceboTrackerDB')

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Resets the jobemulator database.
        """
        self.queries.reset()
        self.placeboTrackerDb.reset()
        parts = payload.split(',') 
        sites = int(parts[0])
        nodesPerSite = int(parts[1])
        for site in xrange(0,sites):
            for node in xrange(0,nodesPerSite):
                nodeName = 'FAKE_site_'+str(site)+'_FAKE_node_'+str(node)
                self.queries.insertWorkerNode(nodeName) 
