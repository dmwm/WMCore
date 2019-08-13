"""
File       : MSManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
             Alan Malta <alan dot malta AT cern dot ch >
Description: MSManager class provides full functionality of the MSManager service.
It provides a interface to reqmgr2ms service and should be
used in service config.py as following

.. doctest::

    # REST interface
    data = views.section_('data')
    data.object = 'WMCore.MicroService.Service.RestApiHub.RestApiHub'
    data.manager = 'WMCore.MicroService.Unified.MSManager.MSManager'
    data.reqmgr2Url = "%s/reqmgr2" % BASE_URL
    data.readOnly = False
    data.verbose = True
    data.interval = 60
    data.rucioAccount = RUCIO_ACCT
    data.dbsUrl = "%s/dbs/%s/global/DBSReader" % (BASE_URL, DBS_INS)
"""
# futures
from __future__ import division, print_function

# system modules
import time

# WMCore modules
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.MicroService.Unified.MSTransferor import MSTransferor
from WMCore.MicroService.Unified.MSMonitor import MSMonitor
from WMCore.MicroService.Unified.TaskManager import start_new_thread


def daemon(func, reqStatus, interval, logger):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(reqStatus)
        except Exception as exc:
            logger.exception("MS daemon error: %s", str(exc))
        time.sleep(interval)


class MSManager(object):
    """
    Entry point for the MicroServices.
    This class manages both transferor and monitoring services.
    """

    def __init__(self, config=None, logger=None):
        """
        Initialize MSManager class with given configuation,
        logger, ReqMgr2/ReqMgrAux/PhEDEx/Rucio objects,
        and start transferor and monitoring threads.
        :param config: reqmgr2ms service configuration
        :param logger:
        """
        self.uConfig = {}
        self.config = config
        self.logger = getMSLogger(getattr(config, 'verbose', False), logger)
        self.services = [] # list of services we'll run
        self._msConfig = None  # will be defined by _parseConfig API call
        self._parseConfig(config)
        self.logger.info(
            "Configuration including default values:\n%s", self.msConfig)

        # initialize transferor class with assigned status as default
        if 'transferor' in self.services:
            self.msTransferor = MSTransferor(self.msConfig, logger=self.logger)
            thname = 'MSTransferor'
            interval = self.msConfig['transferorInterval']
            self.transfThread = start_new_thread(thname, daemon,
                                                 (self.transferor,
                                                  'assigned',
                                                  interval,
                                                  self.logger))
            self.logger.debug(
                "### Running %s thread %s", thname, self.transfThread.running())

        # initialize monitoring class with staging status as default
        if 'monitor' in self.services:
            self.msMonitor = MSMonitor(self.msConfig, logger=self.logger)
            thname = 'MSTransferorMonit'
            interval = self.msConfig['monitorInterval']
            self.monitThread = start_new_thread(thname, daemon,
                                                (self.monitor,
                                                 'staging',
                                                 interval,
                                                 self.logger))
            self.logger.debug(
                "+++ Running %s thread %s", thname, self.monitThread.running())

    def _parseConfig(self, config):
        """
        __parseConfig_
        Parse the MicroService configuration and set any default values.
        :param config: config as defined in the deployment
        """
        self.logger.info("Using the following config:\n%s", config)

        self.msConfig = {}
        self.msConfig['verbose'] = getattr(config, 'verbose', False)
        self.msConfig['group'] = getattr(config, 'group', 'DataOps')
        interval = getattr(config, 'interval', 5 * 60)
        self.msConfig['interval'] = interval
        self.msConfig['transferorInterval'] = \
                getattr(config, 'transferorInterval', interval)
        self.msConfig['monitorInterval'] = \
                getattr(config, 'monitorInterval', interval)
        self.msConfig['readOnly'] = getattr(config, 'readOnly', True)
        self.services = getattr(config, 'services', [])
        if not self.services:
            # if we not provided with specific services we'll run them both
            self.services = ['transferor', 'monitor']

        reqmgr2Url = 'https://cmsweb.cern.ch/reqmgr2'
        self.msConfig['reqmgrUrl'] = getattr(config, 'reqmgr2Url', reqmgr2Url)
        self.msConfig['reqmgrCacheUrl'] = \
            self.msConfig['reqmgrUrl'].replace(
                'reqmgr2', 'couchdb/reqmgr_workload_cache')
        phedexUrl = 'https://cmsweb.cern.ch/phedex/datasvc/json/prod'
        self.msConfig['phedexUrl'] = getattr(config, 'phedexUrl', phedexUrl)
        dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
        self.msConfig['dbsUrl'] = getattr(config, 'dbsUrl', dbsUrl)

    def transferor(self, reqStatus):
        """
        MSManager transferor function.
        It performs Unified logic for data subscription and
        transfers requests from assigned to staging/staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        if 'transferor' in self.services and hasattr(self, 'msTransferor'):
            startT = time.time()
            self.logger.info("Starting the transferor thread...")
            self.msTransferor.execute(reqStatus)
            self.logger.info(
                "Total transferor execution time: %.2f secs", time.time() - startT)

    def monitor(self, reqStatus):
        """
        MSManager monitoring function.
        It performs transfer requests from staging to staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        if 'monitor' in self.services and hasattr(self, 'msMonitor'):
            startT = time.time()
            self.logger.info("Starting the monitor thread...")
            self.msMonitor.execute(reqStatus)
            self.logger.info(
                "Total monitor execution time: %.2f secs", time.time() - startT)

    def stop(self):
        "Stop MSManager"
        # stop MSTransferorMonit thread
        if 'monitor' in self.services and hasattr(self, 'monitThread'):
            self.monitThread.stop()
        # stop MSTransferor thread
        if 'transferor' in self.services and hasattr(self, 'transfThread'):
            self.transfThread.stop()  # stop checkStatus thread
            status = self.transfThread.running()
            return status

    def info(self, reqName):
        "Return info about given request"
        # obtain status records from couchdb for given request
        statusRecords = self.getStatusRecords(reqName)
        # check status records and obtain completion status
        _, completed = self.checkStatusRecords(statusRecords)
        return {'request': reqName, 'status': completed}

    def delete(self, request):
        "Delete request in backend"
        pass

    def status(self, **kwargs):
        """
        Return current status for the MicroService Manager
        Args:
            **kwargs: it will be a request name in the future
        """
        # TODO: eventually give it the correct purpose like, given
        # a request name, return its transfer status
        return "OK"
