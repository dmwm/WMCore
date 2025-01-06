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
    data.manager = 'WMCore.MicroService.MSManager.MSManager'
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
from builtins import object
from time import sleep
from datetime import datetime

# 3rd party modules
from cherrypy import HTTPError

# WMCore modules
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.MicroService.MSCore.TaskManager import start_new_thread
from Utils.Utilities import strToBool


def daemon(func, reqStatus, interval, logger):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(reqStatus)
        except Exception as exc:
            logger.exception("MS daemon error: %s", str(exc))
        sleep(interval)


def daemonOpt(func, interval, logger, *args, **kwargs):
    "Daemon to perform given function action for all request in our store"
    while True:
        try:
            func(*args, **kwargs)
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
        self.statusRuleCleaner = {}
        self.statusUnmerged = {}
        self.statusPileup = {}

        # initialize pileup module
        if 'pileup' in self.services:
            from WMCore.MicroService.MSPileup.MSPileup import MSPileup
            self.msPileup = MSPileup(self.msConfig, logger=self.logger)
            thname = 'MSPileup'
            self.pileupThread = start_new_thread(thname, daemonOpt,
                                                 (self.pileup,
                                                  self.msConfig['interval'],
                                                  self.logger))
            self.logger.info("### Running %s thread %s", thname, self.pileupThread.running())

        # initialize pileup-tasks module
        if 'pileup-tasks' in self.services:
            from WMCore.MicroService.MSPileup.MSPileupTaskManager import MSPileupTaskManager
            self.msPileupTasks = MSPileupTaskManager(self.msConfig, logger=self.logger)
            thname = 'MSPileupTasks'
            self.pileupTasksThread = start_new_thread(thname, daemonOpt,
                                                      (self.pileupTasks,
                                                       self.msConfig['interval'],
                                                       self.logger))
            self.logger.info("### Running %s thread %s", thname, self.pileupTasksThread.running())

        # initialize transferor module
        if 'transferor' in self.services:
            from WMCore.MicroService.MSTransferor.MSTransferor import MSTransferor
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
            from WMCore.MicroService.MSMonitor.MSMonitor import MSMonitor
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
            from WMCore.MicroService.MSOutput.MSOutput import MSOutput
            reqStatus = ['closed-out', 'announced']

            thname = 'MSOutputConsumer'
            self.msOutputConsumer = MSOutput(self.msConfig, mode=thname, logger=self.logger)
            # set the consumer to run twice faster than the producer
            consumerInterval = self.msConfig['interval'] // 2
            self.outputConsumerThread = start_new_thread(thname, daemon,
                                                         (self.outputConsumer,
                                                          reqStatus,
                                                          consumerInterval,
                                                          self.logger))
            self.logger.info("=== Running %s thread %s", thname, self.outputConsumerThread.running())

            thname = 'MSOutputProducer'
            self.msOutputProducer = MSOutput(self.msConfig, mode=thname, logger=self.logger)
            self.outputProducerThread = start_new_thread(thname, daemon,
                                                         (self.outputProducer,
                                                          reqStatus,
                                                          self.msConfig['interval'],
                                                          self.logger))
            self.logger.info("=== Running %s thread %s", thname, self.outputProducerThread.running())

        # initialize rule cleaner module
        if 'ruleCleaner' in self.services:
            from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner
            reqStatus = ['announced', 'aborted-completed', 'rejected']
            self.msRuleCleaner = MSRuleCleaner(self.msConfig, logger=self.logger)
            thname = 'MSRuleCleaner'
            self.ruleCleanerThread = start_new_thread(thname, daemon,
                                                      (self.ruleCleaner,
                                                       reqStatus,
                                                       self.msConfig['interval'],
                                                       self.logger))
            self.logger.info("--- Running %s thread %s", thname, self.ruleCleanerThread.running())

        # initialize unmerged module
        if 'unmerged' in self.services:
            from WMCore.MicroService.MSUnmerged.MSUnmerged import MSUnmerged
            self.msUnmerged = MSUnmerged(self.msConfig, logger=self.logger)
            thname = 'MSUnmerged'
            self.unmergedThread = start_new_thread(thname, daemonOpt,
                                                   (self.unmerged,
                                                    self.msConfig['interval'],
                                                    self.logger))
            self.logger.info("--- Running %s thread %s", thname, self.unmergedThread.running())

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

        self.msConfig['reqmgrCacheUrl'] = self.msConfig['reqmgr2Url'].replace('reqmgr2',
                                                                              'couchdb/reqmgr_workload_cache')

    def pileup(self, *args, **kwargs):
        """
        MSManager pileup function.
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the pileup thread...")
        res = self.msPileup.status()
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total pileup execution time: %.2f secs", res['execution_time'])
        self.statusPileup = res

    def pileupTasks(self, *args, **kwargs):
        """
        MSManager pileup tasks function.
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the pileup-tasks thread...")
        self.msPileupTasks.executeCycle()
        res = self.msPileupTasks.status()
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total pileup-tasks execution time: %.2f secs", res['execution_time'])
        self.statusPileupTasks = res

    def transferor(self, reqStatus):
        """
        MSManager transferor function.
        It performs input data placement logic for workflows that have
        been assigned in the system. Successful workflows will be moved
        to the staging status in ReqMgr2.
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

    def ruleCleaner(self, reqStatus):
        """
        MSManager ruleCleaner function.
        It cleans the block level Rucio rules created by WMAgent and
        performs request status transition from ['announced', 'aborted-completed', 'rejected'] to
        '{normal, aborted, rejected}-archived' state of ReqMgr2.
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-RuleCleaner
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the ruleCleaner thread...")
        res = self.msRuleCleaner.execute(reqStatus)
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total ruleCleaner execution time: %d secs", res['execution_time'])
        self.statusRuleCleaner = res

    def unmerged(self, *args, **kwargs):
        """
        MSManager unmerged function.
        It cleans the Unmerged area of the CMS LFN Namespace
        For references see
        https://github.com/dmwm/WMCore/wiki/ReqMgr2-MicroService-Unmerged
        """
        startTime = datetime.utcnow()
        self.logger.info("Starting the unmerged thread...")
        res = self.msUnmerged.execute()
        endTime = datetime.utcnow()
        self.updateTimeUTC(res, startTime, endTime)
        self.logger.info("Total Unmerged execution time: %d secs", res['execution_time'])
        self.statusUnmerged = res

    def stop(self):
        "Stop MSManager"
        status = None
        # stop MSMonitor thread
        if 'monitor' in self.services and hasattr(self, 'monitThread'):
            self.monitThread.stop()
            status = self.monitThread.running()
        # stop MSPileup thread
        if 'pileup' in self.services and hasattr(self, 'pileupThread'):
            self.pileupThread.stop()  # stop checkStatus thread
            status = self.pileupThread.running()
        # stop MSPileupTasks thread
        if 'pileup-tasks' in self.services and hasattr(self, 'pileupTasksThread'):
            self.pileupTasksThread.stop()  # stop checkStatus thread
            status = self.pileupTasksThread.running()
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
        # stop MSRuleCleaner thread
        if 'ruleCleaner' in self.services and hasattr(self, 'ruleCleanerThread'):
            self.ruleCleanerThread.stop()
            status = self.ruleCleanerThread.running()
        # stop MSUnmerged thread
        if 'unmerged' in self.services and hasattr(self, 'unmergedThread'):
            self.unmergedThread.stop()
            status = self.unmergedThread.running()
        return status

    def info(self, reqName=None, **kwargs):
        """
        Return transfer information for a given request
        :param reqName: request name
        :param kwargs: other arguments that can be provided for different
            microservices. Examples:
            rse: string with the name of the RSE (e.g 'T2_DE_DESY')
        :return: data transfer information for this request
        """
        if 'ruleCleaner' in self.services or 'unmerged' in self.services \
                or 'pileup' in self.services or 'pileup-tasks' in self.services:
            data = {}
        else:
            data = {"request": reqName, "transferDoc": None}
        if reqName:
            transferDoc = None
            # obtain the transfer information for a given request records from couchdb for given request
            if 'monitor' in self.services:
                transferDoc = self.msMonitor.reqmgrAux.getTransferInfo(reqName)
            elif 'transferor' in self.services:
                transferDoc = self.msTransferor.reqmgrAux.getTransferInfo(reqName)
            elif 'output' in self.services:
                transferDoc = self.msOutputProducer.getTransferInfo(reqName)
            if transferDoc:
                # it's always a single document in Couch
                data['transferDoc'] = transferDoc[0]

        if 'unmerged' in self.services:
            detail = strToBool(kwargs.get('detail', False))

            if kwargs.get("rse"):
                data = self.msUnmerged.getStatsFromMongoDB(detail=detail, rse=kwargs['rse'])
        return data

    def get(self, **kwds):
        """
        Fetch data in backend, HTTP GET request
        :param args: input parameters
        :param kwds: positional arguments
        :return: list of results
        """
        if 'pileup' in self.services:
            return self.msPileup.getPileup(**kwds)
        return []

    def post(self, doc):
        """
        Either create or fetch data from backend, HTTP POST request

        :param doc: input JSON doc for HTTP POST request
        :return: list of results
        """
        res = []
        if 'pileup' in self.services:
            if 'pileupName' in doc:
                # this is create POST request
                res = self.msPileup.createPileup(doc)
            if 'query' in doc:
                # this is POST request to get data for a given JSON query
                res = self.msPileup.queryDatabase(doc)
        elif 'transferor' in self.services:
            if 'workflow' in doc:
                res = self.msTransferor.updateSites(doc)
            else:
                raise HTTPError(400, 'invalid payload %s to MSTransferor REST API, expecting {"workflow": <workflow>}' % doc)
        return res

    def update(self, doc):
        """
        Update resource in backend, HTTP PUT request

        :param doc: input JSON doc for HTTP PUT request
        :return: list of results
        """
        if 'pileup' in self.services:
            return self.msPileup.updatePileup(doc)
        return []

    def delete(self, spec):
        """
        Delete resource in backend, HTTP DELETE request

        :param doc: input JSON spec for HTTP DELETE request
        :return: list of results
        """
        if 'pileup' in self.services:
            return self.msPileup.deletePileup(spec)
        return []

    def status(self, detail, **kwargs):
        """
        Return the current status of a MicroService and a summary
        of its last execution activity.
        :param detail: boolean used to retrieve some extra information
          regarding the service
        :param kwargs: other arguments that can be provided for different
            microservices. Examples:
            rse_type: string with the type of the RSEs to fetch (MSUnmerged)
                      Meaningful values at the moment are: "t1", "t2t3" and "t2t3us"
        :return: a dictionary
        """
        data = {"status": "OK"}
        if detail and 'transferor' in self.services:
            data.update(self.statusTrans)
        elif detail and 'pileup' in self.services:
            data.update(self.statusPileup)
        elif detail and 'pileup-tasks' in self.services:
            data.update(self.statusPileupTasks)
        elif detail and 'monitor' in self.services:
            data.update(self.statusMon)
        elif detail and 'output' in self.services:
            data.update(self.statusOutput)
        elif detail and 'ruleCleaner' in self.services:
            data.update(self.statusRuleCleaner)
        elif 'unmerged' in self.services:
            if kwargs.get("rse_type"):
                data.update({"rse_type": kwargs.get("rse_type")})
            if detail:
                data.update(self.statusUnmerged)
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
