#!/usr/bin/env python

"""
Auto generated stub be careful with editing,
Inheritance is preferred.
"""


import threading

from WMCore.Agent.BaseHandler import BaseHandler

from WMCore.WMFactory import WMFactory


class JobEmulator_TrackJobs(BaseHandler):

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        self.factory = WMFactory('factory','')
        myThread = threading.currentThread()
        # load the placebo tracker db interface.
        self.placeboTrackerDb = self.factory.loadObject('WMComponent.JobEmulator.Database.'+myThread.dialect+'.PlaceboTrackerDB')


     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Implement the handler here.
        Assign values to "messageToBePublished","yourTaskId"
        ,"yourPayloadString,"yourActionPayload"".
        Where necessary
        """
        self.placeboTrackerDb.insertJob(str(payload))
