#!/usr/bin/env python

"""
Emulates scheduling of an incoming job.
"""

import logging
import threading

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.WMFactory import WMFactory


#FIXME: needs to be replaced.
from ProdCommon.MCPayloads.JobSpec import JobSpec

class EmulateJob(BaseHandler):
    """
    Emulates scheduling of an incoming job.
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        # load the job scheduler.
        scheduler = self.component.config.JobEmulator.scheduler
        self.factory = WMFactory('factory','')
        self.scheduler = self.factory.loadObject(scheduler)
        # load queries to our backend.
        myThread = threading.currentThread()
        self.queries = self.factory.loadObject('WMComponent.JobEmulator.Database.'+myThread.dialect+'.Sites')

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Emulates scheduling of an incoming job.
        """

        logging.debug("EmulatingJob : %s" % payload)

        jobSpec = JobSpec()

        try:
            jobSpec.load(payload)
        except StandardError, ex:
            logging.error("Error loading JobSpec file: %s" % payload)
            logging.error(str(ex))

        # register job
        # find a suitable node and associate job to it.
        nodeInfo = self.scheduler.allocateJob()
        self.queries.insertJob(jobSpec.parameters['JobName'], jobSpec.parameters['JobType'], payload, nodeInfo[0])
        self.queries.increaseJobCountByNodeID(nodeInfo[0])
