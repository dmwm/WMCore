#!/usr/bin/env python
"""
_MSGTransPortAgent_

MSG TransPortAgent Base Class
Use _dict_ for all transportation

"""



import random
import time
import sys
import types
import os
import time


class MSGTransPortAgent:
    """
    _MSGTransPortAgent_

    Main Class for sending monitoring data via the MSG server similar to apmon
    """

    def __init__ (self, clusterName, nodeName, instanceId = None):
        """
        Constructor
        """
        self.msgmonConf = {}
        self.clusterName = clusterName
        self.nodeName = nodeName
        self.instanceId = instanceId
        self.sys_monitoring  = 0
        self.sys_interval = 200
        self.general_info = 0
        self.job_monitoring = 0
        self.job_interval = 200
        self.setMaxMsgRate = 500
        if self.instanceId == None:
            try:
                for line in os.popen("/sbin/ifconfig"):
                    if line.find('Ether') > -1:
                        self.instanceId = line.split()[4]

            except ValueError:
                self.instanceId = random.randint(0, 0x7FFFFFFE)

    def connect(self):

        """
        initialize via _connect_
        """
        pass


    def send(self, params):
        """
        _send_
        """
        pass


    def disconnect(self):
        """
        _disconnect_
        """
        pass


    def newDestination(self, host, port, password = ''):
        """
        _newDestination_
        """
        pass


    def addProcessToMonitor(self, pid = None, workDir = None):
        """
        _addProcessToMonitor_
        """
        pass


    def removeProcessToMonitor(self, pid = None):
        """
        _removeProcessToMonitor_

        Remove a process from being monitored
        """
        pass
