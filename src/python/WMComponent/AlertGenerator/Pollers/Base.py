"""
Module accommodates base and/or general purpose classes for pollers
within the Alert messaging framework.

"""

import sys
import time
import logging
import threading
import traceback

import psutil
from WMCore.Alerts import API as alertAPI
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sender import Sender


class ProcessDetail(object):
    """
    Class holds details about a particular process, e.g.
    corresponding psutil.Process instance, list of process's children
    also as psutil.Process instances, etc.

    """

    def __init__(self, pid, name):
        self.pid = int(pid)
        self.name = name
        self.proc = psutil.Process(self.pid)
        try:
            self.children = self.proc.children()  # psutil 3.1.1
        except AttributeError:
            self.children = self.proc.get_children()  # psutil 0.6.1
        self.allProcs = [self.proc] + self.children

    def refresh(self):
        """
        Some child processes may have been spawned or finished.
        Update the list of child processes.

        """
        try:
            self.children = self.proc.children()  # psutil 3.1.1
        except AttributeError:
            self.children = self.proc.get_children()  # psutil 0.6.1
        self.allProcs = [self.proc] + self.children


    def getDetails(self):
        childrenPIDs = [c.pid for c in self.children]
        return dict(pid = self.pid, component = self.name,
                    numChildrenProcesses = len(self.children),
                    children = childrenPIDs)



class Measurements(list):
    """
    Information on measurements which are collected over a period of time.

    """
    def __init__(self, numOfMeasurements):
        """
        Instance of polling measurements.
        numOfMeasurements - how many polling measurements shall be collected
            over a period of time before evaluation of the collected values.

        """
        list.__init__(self)
        self._numOfMeasurements = numOfMeasurements


    def clear(self):
        self[:] = []



class BasePoller(threading.Thread):
    """
    Base class for various pollers running as Thread.
    Each poller creates own Sender instance.
    Starting from Thread entry point method run(), methods run
    in different thread contexts. The only shared variable shall
    be _stopFlag.

    """
    def __init__(self, config, generator):
        threading.Thread.__init__(self)
        # it's particular Poller config only
        self.config = config
        # reference to AlertGenerator instance
        self.generator = generator
        # store levels (critical, soft) for critical, soft thresholds correspondence
        # these values are defined in the AlertProcessor config
        # self.levels and self.thresholds has to have the same corresponding order
        # and critical has to be first - if this threshold is caught, no point
        # testing soft one
        # this belongs to the AlertGenerator and is in fact dependent on AlertProcessor
        # by referencing these two values - not sure if to tolerate such dependecy or
        # configure these two values independently in AlertGenerator itself (surely a
        # possible mismatch would make a bit of chaos)
        self.levels = [self.generator.config.AlertProcessor.critical.level,
                       self.generator.config.AlertProcessor.soft.level]

        # critical, soft threshold values
        self.thresholds = [self.config.critical, self.config.soft]

        # pre-generated alert values, but before sending always new instance is created
        # these values are used to update the newly created instance
        dictAlert = dict(Type = "WMAgent",
                         Workload = "n/a",
                         Component = self.generator.__class__.__name__,
                         Source = "<to_overwrite>")
        self.preAlert = alertAPI.getPredefinedAlert(**dictAlert)
        # flag controlling run of the Thread
        self._stopFlag = False
        # thread own sleep time
        self._threadSleepTime = 0.2 # seconds


    def _handleFailedPolling(self, ex):
        """
        Handle (log and send alert) if polling failed.

        """
        trace = traceback.format_exception(*sys.exc_info())
        traceString = '\n '.join(trace)
        errMsg = ("Polling failed in %s, reason: %s" % (self.__class__.__name__, ex))
        logging.error("%s\n%s" % (errMsg, traceString))
        a = Alert(**self.preAlert)
        a.setTimestamp()
        a["Source"] = self.__class__.__name__
        a["Details"] = dict(msg = errMsg)
        a["Level"] = 10
        logging.info("Sending an alert (%s): %s" % (self.__class__.__name__, a))
        self.sender(a)


    def run(self):
        """
        This method is called from the AlertGenerator component instance and is
        entry point for a thread.

        """
        logging.info("Thread %s started - run method." % self.__class__.__name__)
        # when running with multiprocessing, it was necessary to create the
        # sender instance in the same context. Stick to it with threading
        # as well - may create some thread-safety issues in ZMQ ...
        self.sender = Sender(self.generator.config.Alert.address,
                             self.generator.config.Alert.controlAddr,
                             self.__class__.__name__)
        self.sender.register()
        logging.info("Thread %s alert sender created: alert addr: %s "
                     "control addr: %s" %
                     (self.__class__.__name__,
                      self.generator.config.Alert.address,
                      self.generator.config.Alert.controlAddr))
        counter = self.config.pollInterval
        # want to periodically check whether the thread should finish,
        # would be impossible to terminate a sleeping thread
        while not self._stopFlag:
            if counter == self.config.pollInterval:
                # it would feel that check() takes long time but there is
                # specified a delay in case of psutil percentage calls
                try:
                    logging.debug("Poller %s check ..." % self.__class__.__name__)
                    self.check()
                except Exception as ex:
                    self._handleFailedPolling(ex)
            counter -= self._threadSleepTime
            if counter <= 0:
                counter = self.config.pollInterval
            if self._stopFlag:
                break
            time.sleep(self._threadSleepTime)
        logging.info("Thread %s - work loop terminated, finished." % self.__class__.__name__)


    def stop(self):
        """
        Method sets the stopFlag so that run() while loop terminates
        at its next iteration.

        """
        self._stopFlag = True


    def terminate(self):
        """
        Methods added when Pollers were re-implemented to run as
        multi-threaded rather than multiprocessing.
        This would be a slightly blocking call - wait for the thread to finish.

        """
        self._stopFlag = True # keep it here as well in case on terminate method is called
        logging.info("Thread %s terminate ..." % self.__class__.__name__)
        self.join(self._threadSleepTime + 0.1)
        if self.is_alive():
            logging.error("Thread %s refuses to finish, continuing." %
                          self.__class__.__name__)
        else:
            logging.info("Thread %s finished." % self.__class__.__name__)

        # deregister with the receiver
        # (was true for multiprocessing implementation:
        # has to create a new sender instance and unregister the name.
        # self.sender instance was created in different thread in run())

        # TODO revise registering/deregistering business for production ...
        # remove unregistering (it seems to take long and wmcoreD which
        # give only limited time for a component to shutdown, and if entire
        # agent is being shutdown, there is no AlertProcessor to deregister with
        # anyway
        # logging.info("Thread %s sending unregister message ..." % self.__class__.__name__)
        # sender = Sender(self.generator.config.Alert.address,
        #                 self.generator.config.Alert.controlAddr,
        #                 self.__class__.__name__)
        # sender.unregister()
        # # if messages weren't consumed, this should get rid of them
        # del sender

        del self.sender
        logging.info("Thread %s terminate finished." % self.__class__.__name__)



class PeriodPoller(BasePoller):
    """
    Collects samples over a configurable period of time.
    Collected samples are evaluated once in the period and compared
    with soft, resp. critical thresholds.

    """

    # interval for psutil cpu percent usage sampling calls: from psutil doc:
    # it's recommended for accuracy that this function be called with at
    # least 0.1 seconds between calls.
    PSUTIL_INTERVAL = 0.2


    def __init__(self, config, generator):
        BasePoller.__init__(self, config, generator)


    def check(self, pd, measurements):
        """
        Method is used commonly for system properties (e.g. overall CPU) as well
        as for particular process monitoring.
        pd - (processDetail) - information about monitored process, may be None if
            this method is called from system monitoring pollers (e.g. CPU usage).
        measurements - Measurements class instance.

        """
        v = self.sample(pd)
        measurements.append(v)
        avgPerc = None
        if len(measurements) >= measurements._numOfMeasurements:
            # evaluate: calculate average value and react
            avgPerc = round((sum(measurements) / len(measurements)), 2)
            details = dict(period = self.config.period,
                           numMeasurements = len(measurements),
                           average = "%s%%" % avgPerc)
            if pd:
                details.update(pd.getDetails())
            measurements.clear()

            for threshold, level in zip(self.thresholds, self.levels):
                if avgPerc >= threshold:
                    a = Alert(**self.preAlert)
                    a.setTimestamp()
                    a["Source"] = self.__class__.__name__
                    details["threshold"] = "%s%%" % threshold
                    a["Details"] = details
                    a["Level"] = level
                    logging.debug("Sending an alert (%s): %s" % (self.__class__.__name__, a))
                    self.sender(a)
                    break # send only one alert, critical threshold tested first
        if avgPerc != None:
            m = ("%s: measurements result: %s%%" % (self.__class__.__name__, avgPerc))
            logging.debug(m)
