"""
Module accommodates base and/or general purpose classes for pollers
within the Alert messaging framework.

"""

import time
import logging

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



class BasePoller(object):
    """
    Base class for various pollers. Class provides esp. entry point
    poll() method from which poller's life starts in a background process
    and Sender instance.
    
    Methods of this class as well as of the inherited ones run in different
    process contexts. The attributes are not shared and if accessed from both
    contexts, the initial values are taken (as set up in the initial process)
    and then modified in the later (polling) process context.
    
    """
    def __init__(self, config, generator):
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
                               

    def poll(self):
        """
        This method is called from the AlertGenerator component instance and is
        entry point for different process. Sender instance needs to be created
        here. Each poller instance has its own sender instance.
        
        """
        self.sender = Sender(self.generator.config.Alert.address,
                             self.__class__.__name__,
                             self.generator.config.Alert.controlAddr)
        self.sender.register()
        while True:
            # it would feel that check() takes long time but there is
            # specified a delay in case of psutil percentage calls
            self.check()
            time.sleep(self.config.pollInterval)
        
    
    def shutdown(self):
        """
        This method is called from main AlertGenerator process to unregister
        senders with receiver. Has to create a new sender instance and 
        unregister the name. self.sender instance created in poll() is not
        visible to this process.
        
        """
        sender = Sender(self.generator.config.Alert.address,
                        self.__class__.__name__,
                        self.generator.config.Alert.controlAddr)
        sender.unregister()
        
        
        
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
                    # #2238 AlertGenerator test can take 1 hour+ (and fail)
                    # logging from different process context (multiprocessing.Process)
                    # causes issues, own new logging.getLogger not helpful
                    #logging.debug(a)
                    self.sender(a)
                    break # send only one alert, critical threshold tested first
        if avgPerc != None:
            m = ("%s: measurements result: %s%%" % (self.__class__.__name__, avgPerc))
            # #2238 AlertGenerator test can take 1 hour+ (and fail)
            # logging from different process context (multiprocessing.Process)
            # causes issues, own new logging.getLogger not helpful
            #logging.debug(m)