#!/usr/bin/env python
"""
_ApmonTransPort__

ApMon based Transport Layer
Use _dict_ for all transportation

"""
from __future__ import print_function




import random
import time
import sys
import types
import apmon
import os

apmonLoggingLevel = apmon.Logger.INFO


class ApmonTransPort:

    """ _ApmonTransPort_ """

    def __init__(self, clusterName, nodeName, instanceId = None):
        self.apmonConf = {}
        self.clusterName = clusterName
        self.nodeName = nodeName
        self.instanceId = instanceId
        self.host = None
        self.port = None
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


    def send(self, params):
        """
        _send_

        Send multiple parameters to the Monalisa, with last given cluster and node name
        """
        try:
            params.items()
        except AttributeError as ex:
            msg = "ApmonTransPort: Unable to sendAll the params, use _dict_ - "
            msg += str(ex)
            print(msg)
        else:
            self.apmon.sendParams(params)

        return


    def connect(self):
        """
        initialize via _connect_
        """

        self.apmon = apmon.ApMon(self.apmonConf, apmonLoggingLevel)
        self.apmon.setMaxMsgRate(self.setMaxMsgRate)
        self.apmon.sendParameter(self.clusterName, self.nodeName, 'InstanceId', self.instanceId)

        return


    def disconnect(self):
        """
        garbage-collection
        """
        self.apmon.free()

        return


    def newDestination(self, host, port, password = ''):
        """
        _newDistination_
        """

        self.host = host
        self.port = port

        dest = {'%s:%d' %(self.host, self.port) : {'sys_monitoring' : self.sys_monitoring,
                            'general_info'   : self.general_info,
                            'job_monitoring' : self.job_monitoring,
                            'job_interval' : self.job_interval,
                            'sys_interval' : self.sys_interval }}

        self.apmonConf.update(dest)
        dest.clear()

        return


    def addProcessToMonitor(self, pid = None, workDir = None):
        """
        _addProcessToMonitor_
        """

        if workDir == None:
            workDir = os.getcwd()

        if pid == None:
            pid = os.getppid()

        self.job_monitoring = 1
        self.newDestination(self.host, self.port)
        self.connect()
        self.apmon.addJobToMonitor(pid, workDir, self.clusterName, self.nodeName)

        return



    def removeProcessToMonitor(self, pid = None):
        """
        _removeProcessToMonitor_

        Remove a process from being monitored
        """

        if pid == None:
            pid = os.getppid()

        self.apmon.removeJobToMonitor(pid)
        self.disconnect()
        return
