#!/usr/bin/env python

"""
_NATSMonitor_
"""
from __future__ import division

# system modules
import logging
import traceback

# WMCore modules
from WMCore.WMRuntime.Monitors.WMRuntimeMonitor import WMRuntimeMonitor
from WMCore.WMRuntime.NATSInterface import NATSPublisher


class NATSMonitor(WMRuntimeMonitor):
    "NATSMonitoring provide generic interface to NATS for WMAgent"
    def __init__(self):
        self.mgr = None
        WMRuntimeMonitor.__init__(self)

    def initMonitor(self, task, job, logPath, args=None):
        "Initialize NATSMonitor"
        if not args:
            args = {}
        server = args.get('server', '')
        if not server:
            logging.error("Please provide valid NATS server")
        topics = args.get('topics', None)
        attrs = args.get('attrs', None)
        self.mgr = NATSPublisher(task, job, server, topics, attrs)

    def jobStart(self, task=None):
        """
        Job start notifier.
        """
        try:
            self.mgr.jobStart()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

    def jobEnd(self, task):
        """
        Job End notification
        """
        try:
            self.mgr.jobEnd()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

    def stepStart(self, step):
        """
        Step start notification
        """
        try:
            self.mgr.stepStart(step=step)
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

    def stepEnd(self, step, stepReport):
        """
        Step end notification

        """
        try:
            self.mgr.stepEnd(step=step, stepReport=stepReport)
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

    def stepKilled(self):
        """
        Step killed notification
        """
        try:
            self.mgr.stepKilled(step=self.currentStep)
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

    def jobKilled(self):
        """
        Killed job notification
        """
        try:
            self.mgr.jobKilled()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))

    def periodicUpdate(self):
        """
        Run on the defined intervals. Tell the dashboard info to run the
        periodic update
        """
        try:
            self.mgr.periodicUpdate()
        except Exception as ex:
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
