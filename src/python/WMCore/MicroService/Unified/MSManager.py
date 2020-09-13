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
    data.limitRequestsPerCycle = 500
    data.enableStatusTransition = False
    data.enableDataTransfer = False
    data.verbose = True
    data.interval = 60
    data.rucioAccount = RUCIO_ACCT
    data.dbsUrl = "%s/dbs/%s/global/DBSReader" % (BASE_URL, DBS_INS)
"""
# futures
from __future__ import division, print_function

# system modules
from time import sleep
from datetime import datetime
from collections import deque

# WMCore modules
from WMCore.MicroService.Unified.Common import getMSLogger
from WMCore.MicroService.Unified.MSTransferor import MSTransferor
from WMCore.MicroService.Unified.MSMonitor import MSMonitor
from WMCore.MicroService.Unified.MSOutput import MSOutput
from WMCore.MicroService.Unified.TaskManager import start_new_thread


def daemon(func, reqStatus, interval, logger):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(reqStatus)
        except Exception as exc:
            logger.exception("MS daemon error: %s", str(exc))
        sleep(interval)


class MSManager(object):
    """
    Entry point for the MicroServices.
    This class manages both transferor and monitoring services.
    """

    def __init__(self, config=None, logger=None):
        """
        Initialize MSManager class with given configuration,
        logger, ReqMgr2/ReqMgrAux/PhEDEx/Rucio objects,
        and start transferor and monitoring threads.
        :param config: reqmgr2ms service configuration
        :param logger:
        """
        self.config = config
        self.logger = getMSLogger(getattr(config, 'verbose', False), logger)
        self._parseConfig(config)
        self.logger.info("Configuration including default values:\n%s", self.msConfig)
        self.statusTrans = {}
        self.statusMon = {}
        self.statusOutput = {}

        # initialize transferor module
        if 'transferor' in self.services:
            self.msTransferor = MSTransferor(self.msConfig, logger=self.logger)
            thname = 'MSTransferor'
            self.transfThread = start_new_thread(thname, daemon,
                                                 (self.transferor,
                                                  'assigned',
                                                  self.msConfig['interval'],
                                                  self.logger))
            self.logger.info("### Running %s thread %s", thname, self.transfThread.running())

        # initialize monitoring module
        if 'monitor' in self.services:
            self.msMonitor = MSMonitor(self.msConfig, logger=self.logger)
            thname = 'MSMonitor'
            self.monitThread = start_new_thread(thname, daemon,
                                                (self.monitor,
                                                 'staging',
                                                 self.msConfig['interval'],
                                                 self.logger))
            self.logger.info("+++ Running %s thread %s", thname, self.monitThread.running())

        # initialize output module
        if 'output' in self.services:
            reqStatus = ['closed-out', 'announced']
            # thread safe cache to keep the last X requests processed in MSOutput
            requestNamesCached = deque(maxlen=self.msConfig.get("cacheRequestSize", 10000))

            thname = 'MSOutputConsumer'
            self.msOutputConsumer = MSOutput(self.msConfig, mode=thname,
                                             reqCache=requestNamesCached, logger=self.logger)
            # set the consumer to run twice faster than the producer
            consumerInterval = self.msConfig['interval'] // 2
            self.outputConsumerThread = start_new_thread(thname, daemon,
                                                         (self.outputConsumer,
                                                          reqStatus,
                                                          consumerInterval,
                                                          self.logger))
            self.logger.info("=== Running %s thread %s", thname, self.outputConsumerThread.running())

            thname = 'MSOutputProducer'
            self.msOutputProducer = MSOutput(self.msConfig, mode=thname,
                                             reqCache=requestNamesCached, logger=self.logger)
            self.outputProducerThread = start_new_thread(thname, daemon,
                                                         (self.outputProducer,
                                                          reqStatus,
                                                          self.msConfig['interval'],
                                                          self.logger))
            self.logger.info("=== Running %s thread %s", thname, self.outputProducerThread.running())

    def _parseConfig(self, config):
        """
        __parseConfig_
        Parse the MicroService configuration and set any default values.
        :param config: config as defined in the deployment
        """
        self.logger.info("Using the following MicroServices config: %s", config.dictionary_())
        self.services = getattr(config, 'services', [])

        self.msConfig = {}
        self.msConfig.update(config.dictionary_())
        self.msConfig.setdefault("useRucio", False)

        self.msConfig['reqmgrCacheUrl'] = self.msConfig['reqmgr2Url'].replace('reqmgr2',
                                                                              'couchdb/reqmgr_workload_cache')

    def transferor(self, reqStatus):
        """
        MSManager transferor function.
        It performs Unified logic for data subscription and
        transfers requests from assigned to staging/staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the transferor thread...")
        res = self.msTransferor.execute(reqStatus)
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total transferor execution time: %.2f secs", res['execution_time'])
        self.statusTrans = res

    def monitor(self, reqStatus):
        """
        MSManager monitoring function.
        It performs transfer requests from staging to staged state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Transferor
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the monitor thread...")
        res = self.msMonitor.execute(reqStatus)
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total monitor execution time: %d secs", res['execution_time'])
        self.statusMon = res

    def outputConsumer(self, reqStatus):
        """
        MSManager Output Data Placement function.
        It subscribes the output datasets to the Data Management System.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Output
        reqStatus: Status of requests to work on
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the outputConsumer thread...")
        res = self.msOutputConsumer.execute(reqStatus)
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total outputConsumer execution time: %d secs", res['execution_time'])
        self.statusOutput = res

    def outputProducer(self, reqStatus):
        """
        MSManager MongoDB Uploader function.
        It uploads the documents describing a workflow output Data subscription
        into MongoDb. For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Output
        reqStatus: Status of requests to work on
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the outputProducer thread...")
        res = self.msOutputProducer.execute(reqStatus)
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total outputProducer execution time: %d secs", res['execution_time'])
        self.statusOutput = res

    def stop(self):
        "Stop MSManager"
        status = None
        # stop MSMonitor thread
        if 'monitor' in self.services and hasattr(self, 'monitThread'):
            self.monitThread.stop()
            status = self.monitThread.running()
        # stop MSTransferor thread
        if 'transferor' in self.services and hasattr(self, 'transfThread'):
            self.transfThread.stop()  # stop checkStatus thread
            status = self.transfThread.running()
        # stop MSOutput threads
        if 'output' in self.services and hasattr(self, 'outputConsumerThread'):
            self.outputConsumerThread.stop()
            status = self.outputConsumerThread.running()
        if 'output' in self.services and hasattr(self, 'outputProducerThread'):
            self.outputProducerThread.stop()
            status = self.outputProducerThread.running()
        return status

    def info(self, reqName=None):
        """
        Return transfer information for a given request
        :param reqName: request name
        :return: data transfer information for this request
        """
        data = {"request": reqName, "transferDoc": None}
        if reqName:
            # obtain the transfer information for a given request records from couchdb for given request
            if 'monitor' in self.services:
                transferDoc = self.msMonitor.reqmgrAux.getTransferInfo(reqName)
            elif 'transferor' in self.services:
                transferDoc = self.msTransferor.reqmgrAux.getTransferInfo(reqName)
            if transferDoc:
                # it's always a single document in Couch
                data['transferDoc'] = transferDoc[0]
        return data

    def delete(self, request):
        "Delete request in backend"
        pass

    def status(self, detail):
        """
        Return the current status of a MicroService and a summary
        of its last execution activity.
        :param detail: boolean used to retrieve some extra information
          regarding the service
        :return: a dictionary
        """
        data = {"status": "OK"}
        if detail and 'transferor' in self.services:
            data.update(self.statusTrans)
        elif detail and 'monitor' in self.services:
            data.update(self.statusMon)
        elif detail and 'output' in self.services:
            data.update(self.statusOutput)
        return data

    def updateTimeUTC(self, reportDict, startT, endT):
        """
        Given a report summary dictionary and start/end time, update
        the report with human readable timing information
        :param reportDict: summary dictionary
        :param startT: epoch start time for a given service
        :param endT: epoch end time for a given service
        """
        reportDict['start_time'] = startT.strftime("%a, %d %b %Y %H:%M:%S UTC")
        reportDict['end_time'] = endT.strftime("%a, %d %b %Y %H:%M:%S UTC")
        reportDict['execution_time'] = (endT - startT).total_seconds()
