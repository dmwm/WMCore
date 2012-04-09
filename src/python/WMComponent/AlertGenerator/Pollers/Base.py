"""
Module accommodates base and/or general purpose classes for pollers
within the Alert messaging framework.

"""

import time
import logging
import threading

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
        # the list of processes is created during start-up, it may be desirable
        # to refresh this list of subprocesses in the course of component process
        # checking if more processes are spawned values are during component's life-time
        self.children = self.proc.get_children()
        self.allProcs = [self.proc] + self.children


    def getDetails(self):
        return dict(pid = self.pid, component = self.name,
                    numChildrenProcesses = len(self.children))
        
        

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
        # and critical has to be first - if this threshold is caught, no point testing soft one
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
        self._threadSleepTime = 0.5 # seconds
        

    def run(self):
        """
        This method is called from the AlertGenerator component instance and is
        entry point for a thread. 
        
        """
        # when running with multiprocessing, this was necessary, stick to it
        # with threading as well - may create some thread-safety issues in ZMQ ...
        self.sender = Sender(self.generator.config.Alert.address,
                             self.__class__.__name__,
                             self.generator.config.Alert.controlAddr)
        self.sender.register()
        counter = self.config.pollInterval
        # want to periodically check whether the thread should finish,
        # would be impossible to terminate a sleeping thread
        while not self._stopFlag:
            if counter == self.config.pollInterval:
                # it would feel that check() takes long time but there is
                # specified a delay in case of psutil percentage calls                
                self.check()
                counter -= self._threadSleepTime
                if counter <= 0:
                    counter = self.config.pollInterval
            if self._stopFlag:
                break
            time.sleep(self._threadSleepTime)
                    
            
    def terminate(self):
        """
        Methods added when Pollers were reimplemented to run as
        multi-threaded rather than multiprocessing.
        This would be a slightly blocking call - wait for the thread to finish.
        
        """
        self._stopFlag = True
        self.join(self._threadSleepTime + 0.1)
        if self.is_alive():
            logging.error("Thread %s refuses to finish, continuing." % self.__class__.__name__)
        else:
            logging.debug("Thread %s finished." % self.__class__.__name__)
            
        # deregister with the receiver
        # (was true for multiprocessing implemention:
        # has to create a new sender instance and unregister the name. 
        # self.sender instance was created in different thread in run())
        sender = Sender(self.generator.config.Alert.address,
                        self.__class__.__name__,
                        self.generator.config.Alert.controlAddr)
        sender.unregister()
        # if messages weren't consumed, this should get rid of them
        del sender
        del self.sender
        
         
        
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
                    a["Source"] = self.__class__.__name__
                    a["Timestamp"] = time.time()
                    details["threshold"] = "%s%%" % threshold
                    a["Details"] = details                    
                    a["Level"] = level
                    logging.debug(a)
                    self.sender(a)
                    break # send only one alert, critical threshold tested first
        if avgPerc != None:
            m = ("%s: measurements result: %s%%" % (self.__class__.__name__, avgPerc))
            logging.debug(m)