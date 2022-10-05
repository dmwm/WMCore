#!/usr/bin/env python
"""
DBS Buffer handler for JobSuccess event
"""
__all__ = []





from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.Agent.Configuration import loadConfigurationFile

#from WMComponent.DBSBuffer.Database.Interface.addToBuffer import AddToBuffer
from WMCore.WMFactory import WMFactory

#from WMComponent.ErrorHandler.Handler.DefaultSlave import DefaultSlave
from WMCore.ThreadPool.ThreadSlave import ThreadSlave

#Now I need the stuff that does the work
from WMCore.WMSpec.Makers.Interface.CreateWorkArea import CreateWorkArea


import os
import string
import logging
import time
import threading



class MakeJobSlave(ThreadSlave):
    """
    Slave class for making jobs.

    """


    def __call__(self, parameters):
        """
        Handles the event with payload, by sending it to the threadpool.
        """

        myThread = threading.currentThread()
        #myThread.transaction.begin()

        #payload = parameters #I think this is correct; it's all that I've got to go on
        factory = WMFactory("JobMaker", "WMCore.WMSpec.Makers.Interface")
        createWorkArea=factory.loadObject("CreateWorkArea")
        jobGroupID = parameters #There should only be one


        #We should have, in the payload, exactly one jobGroup ID
        createWorkArea.getNewJobGroup(jobGroupID)
        createWorkArea.createJobGroupArea()
        createWorkArea.createWorkArea()

        #For testing purposes only
        #DO NOT LEAVE ACTIVE!
        #createWorkArea.cleanUpAll()


        #myThread.transaction.commit()

        return
