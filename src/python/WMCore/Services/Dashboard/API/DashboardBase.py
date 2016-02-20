#!/usr/bin/env python
"""
_DashboardBase_

Monitoring plugin to broadcast data to the CMS Dashboard
Allow only to publish a dictionary

"""
from __future__ import absolute_import
from __future__ import print_function



import uuid
import types
import time
import os
from .MSGTransPortAgent import *
from .ApmonTransPort import *

def generateCMSJobID():
    """
    _generateCMSJobID_

    Generate a global job ID at the UI
    """
    return repr(uuid.uuid4())

class DashboardBase(MSGTransPortAgent, ApmonTransPort, dict):
    """
    _DashboardBase_

    Base Class for Dashboard inhert from MSGTransPortAgent & ApmonTransPort
    """

    def __init__(self, publisher=[]):
        """
        constructor class
        """
        dict.__init__(self)
        self.jobid = None
        self.taskid = None
        self.destinations = {}
        self.FILES = {}
        self.publisher = publisher
        if len(self.publisher) != 0:
            if self.jobid == None:
                for members in self.publisher:
                    self.jobid = members.clusterName
                    self.taskid = members.nodeName

    def publish(self, redundancy = 1):

        """
        _publish_

        Publish information in this object to the Dashboard using either or both
        MSG or ML as the transport layer.

        Need to have atleast one destinations

        Redunancy is the number of times to publish the dictionary

        """
        if len(self.publisher) == 0:
            self._InitPublisher()

        params = {}
        params.update(self)
        for key, value in params.items():
            if value == None:
                del params[key]

        for members in self.publisher:
            for i in range(1, redundancy+1):
                members.connect()
                members.send(params)
                members.disconnect()
        return


    def bulkPublish(self, params):

        """
        _bulkPublish_

        Publish information in this object to the Dashboard using either or both
        MSG or ML as the transport layer.

        Need to have atleast one destinations

        Redunancy is the number of times to publish the dictionary

        """
        if len(self.publisher) == 0:
            self._InitPublisher()

        for key, value in params.items():
            if value == None:
                del params[key]

        for members in self.publisher:
            members.connect()
            members.send(params)
            members.disconnect()
        return



    def addDestination(self, host, port):
        """
        _addDestination_ for ML or MSG
        """

        self.destinations[host] = port

        if len(self.publisher) == 0:
            self._InitPublisher()

        for members in self.publisher:
            members.newDestination(host, port)

        return


    def removeProcessToMonitor(self, pid = None):
        """
        _removeProcessToMonitor_

        Remove a process from being monitored - default is self
        """

        if pid == None:
            pid = os.getppid()

        if len(self.publisher) == 0:
            self._InitPublisher()

        for members in self.publisher:
            members.removeProcessToMonitor(pid)

        return


    def addProcessToMonitor(self, pid = None, workDir = None):
        """
        _addProcessToMonitor_
        """

        if workDir == None:
            workDir = os.getcwd()

        if pid == None:
            pid = os.getppid()

        if len(self.publisher) == 0:
            self._InitPublisher()

        for members in self.publisher:
            members.addProcessToMonitor(pid, workDir)

        return


    def getFileAccess(self, lfn, SE, exitStatus = 0):
        """
        _getFileAccess_

        This information will be sent from the WN for each file that was accessed by the job
        """

        if exitStatus != 0:
            SE = SE + "_failed"
        else:
            SE = SE + "_success"


        if SE in self.FILES:
            self.FILES[SE].append(lfn)
        else:
            self.FILES[SE] = [lfn]

        return


    def reportFileAccess(self):
        """
        _reportFileAccess_

        This information will be sent from the WN as the file access report
        """

        if len(self.publisher) == 0:
            self._InitPublisher()

        params = {}
        if len(self.FILES) != 0:
            for key, value in self.FILES.items():
                params[key] = str(value[1:]).strip('[]').replace(',', '')

            for members in self.publisher:
                members.connect()
                members.send(params)
                members.disconnect()

        return


    def emptyClone(self):
        """
        _emptyClone_

        Return a copy of self including only the task, job and destination
        information

        """
        result = DashboardBase()
        result.clear()
        result.taskid = self.taskid
        result.jobid = self.jobid
        result.destinations = self.destinations

        return result



    def _InitPublisher(self):

        """
        _InitPublisher_

        *private*

        Initialise the instance, make sure that task and job attributes are set

        """

        if self.taskid == None:
            msg = "Error: You must set the taskid before adding \n"
            msg += "destinations or publishing data"
            raise RuntimeError(msg)

        if self.jobid == None:
            msg = "Error: You must set the jobid before adding \n"
            msg += "destination or publishing data"
            raise RuntimeError(msg)

        if len(self.destinations) == 0:
            msg = "Error: You must set the destination:port in addDestination "
            raise RuntimeError(msg)

        if len(self.publisher) == 0:
            self.publisher = [ApmonTransPort(self.taskid, self.jobid)]
            print("DashboardBase: Using default transport agent as ML server")
        else:
            for inst in self.publisher:
                if isinstance(inst, ApmonTransPort):
                    print("DashboardBase: Using transport agent as ML server")
                elif isinstance(inst, MSGTransPortAgent):
                    print("DashboardBase: Using transport agent as MSG server")

        return
