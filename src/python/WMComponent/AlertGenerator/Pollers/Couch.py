"""
Module for all CouchDb related polling.

"""


import os
import logging
import types
import psutil

from WMCore.Database.CMSCouch import CouchServer
from WMComponent.AlertGenerator.Pollers.Base import PeriodPoller
from WMComponent.AlertGenerator.Pollers.Base import Measurements
from WMCore.Alerts.Alert import Alert
from WMComponent.AlertGenerator.Pollers.Base import BasePoller
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.System import DirectorySizePoller
from WMComponent.AlertGenerator.Pollers.System import ProcessCPUPoller
from WMComponent.AlertGenerator.Pollers.System import ProcessMemoryPoller



class CouchPoller(PeriodPoller):
    """
    Common class for Couch CPU, memory utilisation monitoring and possibly
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
        Return the PID number of the Couch main server process.
        Standard / default location of log / PID files is:
            /var/log/couchdb/<ver>/couch.log
            /var/run/couchdb/couchdb.pid

        WMAgent Deployment/wmagent/manage defines $INSTALL_COUCH/logs/
        in which log files and PID file are stored.

        First try to read '/_config' query result and check if
        $INSTALL_COUCH/logs/couchdb.pid exists (location of PID file derived
        from log file location / log directory).
        If such PID file does not exist, try default Couch PID file location.

        """
        logging.info("Reading CouchDB PID file ...")
        pidFileName = "couchdb.pid"
        pidFileDefault = os.path.join("/var/run/couchdb", pidFileName)
        try:
            couchURL = getattr(self.config, "couchURL", None)
            if not couchURL:
                raise Exception("Configuration value 'couchURL' missing, can't connect to CouchDB.")
            couch = CouchServer(couchURL)
            r = couch.makeRequest("/_config")
            logFile = r["log"]["file"]
            # derive location of the PID file from full path log file name
            dir = os.path.dirname(logFile)
            pidFile = os.path.join(dir, pidFileName)

            if os.path.exists(pidFile):
                pidStr = open(pidFile, 'r').read()
                pid = int(pidStr)
            else:
                pidStr = open(pidFileDefault, 'r').read()
                pid = int(pidStr)
        except Exception, ex:
            logging.error("%s: could not get CouchDB PID, reason: %s" %
                          (self.__class__.__name__, ex))
            raise
        logging.info("CouchDB server PID: %s" % pid)
        return pid


    def _setUp(self):
        """
        Query the database to find out the main process PID,
        create ProcessDetail and Measurements instances.

        """
        pid = self._getProcessPID()
        self._dbProcessDetail = ProcessDetail(pid, "CouchDB")
        numOfMeasurements = round(self.config.period / self.config.pollInterval, 0)
        self._measurements = Measurements(numOfMeasurements)


    def check(self):
        """
        Above, the database server psutil.Process instance creation may have
        failed. Proceed with checking only if the instance exists.

        """
        if self._dbProcessDetail:
            try:
                PeriodPoller.check(self, self._dbProcessDetail, self._measurements)
            except psutil.error.NoSuchProcess as ex:
                logging.warn(ex)
                logging.warn("Updating info about the polled process ...")
                self._setUp()



class CouchDbSizePoller(DirectorySizePoller):
    """
    Class implements monitoring / polling of the CouchDb database size.

    """
    def __init__(self, config, generator):
        DirectorySizePoller.__init__(self, config, generator)
        self._query = "/_config" # couch query to retrieve configuration info
        # database directory to monitor
        self._dbDirectory = self._getDbDir()


    def _getDbDir(self):
        """
        Connect to CouchDB instance and query its database directory name.

        """
        couchURL = getattr(self.config, "couchURL", None)
        try:
            couch = CouchServer(couchURL)
            r = couch.makeRequest(self._query)
            dataDir = r["couchdb"]["database_dir"]
        except Exception, ex:
            msg = ("%s: could not find out database directory, reason: %s "
                   "couchURL: '%s'" % (self.__class__.__name__, ex, couchURL))
            raise Exception(msg)
        return dataDir



class CouchMemoryPoller(CouchPoller):
    """
    CouchDB memory utilisation poller.

    """
    def __init__(self, config, generator):
        CouchPoller.__init__(self, config, generator)


    @staticmethod
    def sample(processDetail):
        """
        Return a single float representing percentage usage of the main
        memory by the process.

        """
        return ProcessMemoryPoller.sample(processDetail)



class CouchCPUPoller(CouchPoller):
    """
    Monitoring of CouchDb CPU usage. Monitors the main processes
    and its subprocesses.

    """
    def __init__(self, config, generator):
        CouchPoller.__init__(self, config, generator)


    @staticmethod
    def sample(processDetail):
        """
        Return a single float representing CPU usage of the main process
        and its subprocesses.

        """
        return ProcessCPUPoller.sample(processDetail)



class CouchErrorsPoller(BasePoller):
    """
    Polling CouchDb statistics values - number of status error codes
    (configurable).
    """
    def __init__(self, config, generator):
        """
        couch - instance of CouchServer class

        """
        BasePoller.__init__(self, config, generator)
        self.couch = None
        self._query = "/_stats" # couch query to retrieve statistics
        self._setUp()


    def _setUp(self):
        """
        Instantiate CouchServer reference.
        Test connection with CouchDB (first connect and retrieve attempt).

        """
        try:
            couchURL = getattr(self.config, "couchURL", None)
            if not couchURL:
                raise Exception("Configuration value 'couchURL' missing, can't connect to CouchDB.")
            self.couch = CouchServer(couchURL)
            # retrieves result which is not used during this set up
            r = self.couch.makeRequest(self._query)
        except Exception, ex:
            msg = ("%s: could not connect to CouchDB, reason: %s" %
                   (self.__class__.__name__, ex))
            raise Exception(msg)
        # observables shall be list-like integers
        if not isinstance(self.config.observables, (types.ListType, types.TupleType)):
            self.config.observables = tuple([self.config.observables])


    def sample(self, code):
        """
        Make a query to CouchDB and retrieve number of occurrences of
        particular HTTP code as reported by the internal statistics.
        If such HTTP codes has not occurred since the server start,
        if may not have an entry in the statistics result.
        code - string value of the code

        """
        response = self.couch.makeRequest(self._query)
        try:
            statusCodes = response["httpd_status_codes"]
            statusCode = statusCodes[code]
            return statusCode["current"] # another possibility to watch "count"
        except KeyError:
            return None


    def check(self):
        """
        Method called from the base class.
        Iterate over all HTTP status listed in observable config value
        and check number of occurrences of each by querying statistics
        of CouchDB.

        """
        for code in self.config.observables:
            occurrences = self.sample(str(code))
            if occurrences is not None:
                for threshold, level in zip(self.thresholds, self.levels):
                    if occurrences >= threshold:
                        details = dict(HTTPCode = code,
                                       occurrences = occurrences,
                                       threshold = threshold)
                        a = Alert(**self.preAlert)
                        a.setTimestamp()
                        a["Source"] = self.__class__.__name__
                        a["Details"] = details
                        a["Level"] = level
                        logging.debug("Sending an alert (%s): %s" % (self.__class__.__name__, a))
                        self.sender(a)
                        break # send only one alert, critical threshold tested first
            m = ("%s: checked code:%s current occurrences:%s" %
                 (self.__class__.__name__, code, occurrences))
            logging.debug(m)
