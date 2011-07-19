"""
Common module for all MySQL related checked metrics.

"""


import threading
import logging

import psutil

from WMComponent.AlertGenerator.Pollers.Base import PeriodPoller
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.System import DirectorySizePoller
from WMComponent.AlertGenerator.Pollers.System import ProcessCPUPoller
from WMComponent.AlertGenerator.Pollers.System import ProcessMemoryPoller



class MySQLPoller(PeriodPoller):
    """
    Common class for MySQL CPU, memory utilisation monitoring and possibly
    further future properties.
    
    """    
    def __init__(self, config, generator):
        PeriodPoller.__init__(self, config, generator)
        # ProcessDetail class instance (main process plus subprocesses)
        self._dbProcessDetail = None
        # instance of Measurements
        self._measurements = None
        self._setUp()
        
        
    def _getProcessPID(self):
        """
        Query the database and find out its PID file and read the PID number.
        
        """
        myThread = threading.currentThread()
        query = "SHOW VARIABLES LIKE 'pid_file'"
        try:
            # this call will fail if database (dbi) is not set properly
            proxy = myThread.dbi.connection().execute(query)
            result = proxy.fetchone()
            pidFile = result[1]
            pidStr = open(pidFile, 'r').read()
            pid = int(pidStr)
            return pid
        except Exception, ex:
            logging.error("%s: could not read database PID, reason: %s" % (self.__class__.__name__, ex))
            raise
    
    
    def _setUp(self):
        """
        Query the database to find out the main process PID,
        create ProcessDetail and Measurements instances.
        
        """ 
        pid = self._getProcessPID()
        try:
            self._dbProcessDetail = ProcessDetail(pid, "MySQL")
        except (psutil.error.NoSuchProcess, psutil.error.AccessDenied), ex:
            logging.error("%s: polling not possible, reason: %s" % (self.__class__.__name__, ex))
            return
        numOfMeasurements = round(self.config.period / self.config.pollInterval, 0)
        self._measurements = Measurements(numOfMeasurements)
                
        
    def check(self):
        """
        Above, the database server psutil.Process instance creation may have
        failed. Proceed with checking only if the instance exists.
        
        """
        if self._dbProcessDetail:
            PeriodPoller.check(self, self._dbProcessDetail, self._measurements)



class MySQLDbSizePoller(DirectorySizePoller):
    """
    MySQL database directory size poller.
    
    """
    def __init__(self, config, generator):
        DirectorySizePoller.__init__(self, config, generator)
        # database directory to monitor
        self._dbDirectory = self._getDbDir()
        
        
    def _getDbDir(self):
        """
        Connect to the database and query its variables to find out the
        database directory variable.
        
        """
        myThread = threading.currentThread()
        query = "SHOW VARIABLES LIKE 'datadir'"
        try:
            # this call will fail on dbi should not the database be properly set up
            proxy = myThread.dbi.connection().execute(query)            
            result = proxy.fetchone()
            dataDir = result[1]
        except Exception, ex:
            logging.error("%s: could not find out database directory, reason: %s" %
                          (self.__class__.__name__, ex))
            raise
        return dataDir



class MySQLMemoryPoller(MySQLPoller):
    """
    MySQL CPU utilisation poller.
    
    """
    def __init__(self, config, generator):
        MySQLPoller.__init__(self, config, generator)
        
        
    @staticmethod 
    def sample(processDetail):
        """
        Return a single float representing percentage usage of the main
        memory by the process.
        
        """
        return ProcessMemoryPoller.sample(processDetail)
            


class MySQLCPUPoller(MySQLPoller):
    """
    Monitoring of CPU usage of MySQL database main process and its subprocesses.
    
    """
    def __init__(self, config, generator):
        MySQLPoller.__init__(self, config, generator)
        
        
    @staticmethod 
    def sample(processDetail):
        """
        Return a single float representing CPU usage of the main process
        and its subprocesses.
        
        """
        return ProcessCPUPoller.sample(processDetail)