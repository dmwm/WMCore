"""
File       : MSOtput.py

Description: MSOutput.py class provides the whole logic behind
the Output data placement in WMCore MicroServices.
"""

# futures
from __future__ import division, print_function

# system modules
from pymongo import IndexModel, ReturnDocument, errors
from pymongo.command_cursor import CommandCursor
from pprint import pformat
from copy import deepcopy
from time import time
from socket import gethostname
from threading import current_thread
from retry import retry

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import OUTPUT_PRODUCER_REPORT
from WMCore.MicroService.DataStructs.DefaultStructs import OUTPUT_CONSUMER_REPORT
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.Services.DDM.DDM import DDM, DDMReqTemplate
from WMCore.Services.CRIC.CRIC import CRIC
from Utils.EmailAlert import EmailAlert
from Utils.Pipeline import Pipeline, Functor
from WMCore.Database.MongoDB import MongoDB
from WMCore.MicroService.DataStructs.MSOutputTemplate import MSOutputTemplate
from WMCore.MicroService.Unified.MSOutputStreamer import MSOutputStreamer
from WMCore.WMException import WMException


class MSOutputException(WMException):
    """
    General Exception Class for MSOutput Module in WMCore MicroServices
    """
    def __init__(self, message):
        self.myMessage = "MSOtputException: %s" % message
        super(MSOutputException, self).__init__(self.myMessage)


class EmptyResultError(MSOutputException):
    """
    A MSOutputException signalling an empty result from database query.
    """
    def __init__(self, message=None):
        if message:
            self.myMessage = "EmptyResultError: %s"
        else:
            self.myMessage = "EmptyResultError."
        super(EmptyResultError, self).__init__(self.myMessage)


class MSOutput(MSCore):
    """
    MSOutput.py class provides the whole logic behind the Output data placement
    in MicroServices.
    """

    def __init__(self, msConfig, mode, logger=None):
        """
        Runs the basic setup and initialization for the MSOutput module
        :microConfig: microservice configuration
        :mode: MSOutput Run mode:
            - MSOutputConsumer:
                Reads The workflow and transfer subscriptions from MongoDB and
                makes transfer subscriptions.
            - MSOutputProducer:
                Fetches Workflows in a given status from Reqmgr2 then creates
                and uploads the documents to MongoDB.
        """
        super(MSOutput, self).__init__(msConfig, logger)

        self.mode = mode
        self.msConfig.setdefault("limitRequestsPerCycle", 500)
        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 600)
        self.msConfig.setdefault("services", ['output'])
        self.msConfig.setdefault("defaultDataManSys", "DDM")
        self.msConfig.setdefault("defaultGroup", "DataOps")
        self.msConfig.setdefault("enableAggSubscr", True)
        self.msConfig.setdefault("enableDataPlacement", False)
        self.msConfig.setdefault("excludeDataTier", ['NANOAOD'])
        self.msConfig.setdefault("rucioAccount", 'wma_test')
        self.msConfig.setdefault("mongoDBUrl", 'mongodb://localhost')
        self.msConfig.setdefault("mongoDBPort", 8230)
        self.msConfig.setdefault("streamerBufferFile", None)
        self.uConfig = {}
        self.emailAlert = EmailAlert(self.msConfig)

        self.cric = CRIC(logger=self.logger)
        self.uConfig = {}
        self.campaigns = {}
        self.psn2pnnMap = {}

        msOutIndex = IndexModel('RequestName', unique=True)
        msOutDBConfig = {
            'database': 'msOutDB',
            'server': self.msConfig['mongoDBUrl'],
            'port': self.msConfig['mongoDBPort'],
            'logger': self.logger,
            'create': True,
            'collections': [
                ('msOutRelValColl', msOutIndex),
                ('msOutNonRelValColl', msOutIndex)]}

        self.msOutDB = MongoDB(**msOutDBConfig).msOutDB
        self.msOutRelValColl = self.msOutDB['msOutRelValColl']
        self.msOutNonRelValColl = self.msOutDB['msOutNonRelValColl']
        self.ddm = DDM(url=self.msConfig['ddmUrl'],
                       logger=self.logger,
                       enableDataPlacement=self.msConfig['enableDataPlacement'])

    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the output logic, e.g.:
        * unified configuration
        """
        self.logger.info("Updating local cache information.")
        self.uConfig = self.unifiedConfig()
        campaigns = self.reqmgrAux.getCampaignConfig("ALL_DOCS")
        self.psn2pnnMap = self.cric.PSNtoPNNMap()
        if not self.uConfig:
            raise RuntimeWarning("Failed to fetch the unified configuration")
        elif not campaigns:
            raise RuntimeWarning("Failed to fetch the campaign configurations")
        elif not self.psn2pnnMap:
            raise RuntimeWarning("Failed to fetch PSN x PNN map from CRIC")
        else:
            # let's make campaign look-up easier and more efficient
            self.campaigns = {}
            for camp in campaigns:
                self.campaigns[camp['CampaignName']] = camp

    def execute(self, reqStatus):
        """
        Executes the whole output data placement logic
        :return: summary
        """

        # start threads in MSManager which should call this method
        # NOTE:
        #    Here we should make the whole logic - like:
        #    * Calling the system to fetch the workflows from;
        #    * Creating the workflow objects;
        #    * Pushing them into the back end database system we choose for bookkeeping
        #    * Updating their status in that system, both MsStatus (subscribed,
        #      processing, etc.) and also the Reqmgr status
        #    * Associate and keep track of the requestID/subscriptionID/ruleID
        #      returned by the Data Management System and the workflow
        #      object (through the bookkeeping machinery we choose/develop)
        self.currHost = gethostname()
        self.currThread = current_thread()
        self.currThreadIdent = "%s:%s@%s" % (self.currThread.name, self.currThread.ident, self.currHost)

        if self.mode == 'MSOutputProducer':
            summary = self._executeProducer(reqStatus)

        elif self.mode == 'MSOutputConsumer':
            summary = self._executeConsumer()

        else:
            msg = "MSOutput is running in unsupported mode: %s\n" % self.mode
            msg += "Skipping the current run!"
            self.logger.warning(msg)

        return summary

    def _executeProducer(self, reqStatus):
        """
        The function to update caches and to execute the Producer function itslef
        """
        summary = dict(OUTPUT_PRODUCER_REPORT)
        self.updateReportDict(summary, "thread_id", self.currThreadIdent)
        msg = "{}: MSOutput is running in mode: {}".format(self.currThreadIdent, self.mode)
        self.logger.info(msg)

        try:
            requestRecords = {}
            for status in reqStatus:
                numRequestRecords = len(requestRecords)
                requestRecords.update(self.getRequestRecords(status))
                msg = "{}: Retrieved {} requests in status {} from ReqMgr2. ".format(self.currThreadIdent,
                                                                                     len(requestRecords) - numRequestRecords,
                                                                                     status)
                self.logger.info(msg)
        except Exception as err:  # general error
            msg = "{}: Unknown exception while fetching requests from ReqMgr2. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(err))
            self.logger.exception(msg)

        try:
            self.updateCaches()
        except RuntimeWarning as ex:
            msg = "{}: All retries exhausted! Last error was: '{}'".format(self.currThreadIdent,
                                                                           str(ex))
            msg += "\nRetrying to update caches again in the next cycle."
            self.logger.error(msg)
            self.updateReportDict(summary, "error", msg)
            return summary
        except Exception as ex:
            msg = "{}: Unknown exception updating caches. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

        try:
            streamer = MSOutputStreamer(bufferFile=self.msConfig['streamerBufferFile'],
                                        requestRecords=requestRecords,
                                        logger=self.logger)
            total_num_requests = self.msOutputProducer(streamer())
            msg = "{}: Total {} requests processed from the streamer. ".format(self.currThreadIdent,
                                                                               total_num_requests)
            self.logger.info(msg)
            self.updateReportDict(summary, "total_num_requests", total_num_requests)
        except Exception as ex:
            msg = "{}: Unknown exception while running the Producer thread. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        return summary

    def _executeConsumer(self):
        """
        The function to execute the Consumer function itslef
        """

        summary = dict(OUTPUT_CONSUMER_REPORT)
        self.updateReportDict(summary, "thread_id", self.currThreadIdent)
        msg = "{}: MSOutput is running in mode: {} ".format(self.currThreadIdent, self.mode)
        self.logger.info(msg)
        msg = "{}: Service set to process up to {} requests ".format(self.currThreadIdent,
                                                                     self.msConfig["limitRequestsPerCycle"])
        msg += "per cycle per each type 'RelVal' and 'NonRelval' workflows."
        self.logger.info(msg)

        if not self.msConfig['enableDataPlacement']:
            msg = "{} enableDataPlacement = False. ".format(self.currThreadIdent)
            msg += "Running the MSOutput service in dry run mode"
            self.logger.warning(msg)

        try:
            total_num_requests = self.msOutputConsumer()
            msg = "{}: Total {} requests processed. ".format(self.currThreadIdent,
                                                             total_num_requests)
            self.logger.info(msg)
            self.updateReportDict(summary, "total_num_requests", total_num_requests)
        except Exception as ex:
            msg = "{}: Unknown exception while running Consumer thread. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        return summary

    def makeSubscriptions(self, workflow):
        """
        The common function to make the final subscriptions. It depends on the
        default Data Management System configured through msConfig. Based on that
        The relevant service wrapper is called.
        :return: A list of results from the REST interface of the DMS in question
        """

        # NOTE:
        #    Here is just an example construction of the function. None of the
        #    data structures used to visualise it is correct. To Be Updated
 
        if self.msConfig['defaultDataManSys'] == 'DDM':
            # NOTE:
            #    We always aggregate per workflow here (regardless of enableAggSubscr)
            #    and then if we work in strides and enableAggSubscr is True then
            #    we will aggregate all similar subscription for all workflows
            #    in a single subscription - then comes the mess how to map back
            #    which workflow's outputs went to which transfer subscription etc.
            #    (TODO:)
            #
            # NOTE:
            #    Once we move to working in strides of multiple workflows at a time
            #    then the workflow sent to that function should not be a single one
            #    but an iterator of length 'stride' and then we should be doing:
            #    for workflow in workflows:
            if isinstance(workflow, MSOutputTemplate):
                ddmReqList = []
                try:
                    if workflow['isRelVal']:
                        for dMap in workflow['destinationOutputMap']:
                            try:
                                ddmRequest = DDMReqTemplate('copy',
                                                            item=dMap['datasets'],
                                                            n=workflow['numberOfCopies'],
                                                            site=dMap['destination'],
                                                            group='RelVal')
                            except KeyError as ex:
                                # NOTE:
                                #    If we get to here it is most probably because the 'site'
                                #    mandatory field to the DDM request is missing (due to an
                                #    'ALCARECO' dataset or similar). Since this is expected
                                #    to happen a lot, we'd better just log a warning and continue
                                msg = "Could not create DDMReq for Workflow: {}".format(workflow['RequestName'])
                                msg += "Error: {}".format(ex)
                                self.logger.warning(msg)
                                continue
                            ddmReqList.append(ddmRequest)
                    else:
                        # FIXME:
                        #    We need to create the campaignMap and use it for
                        #    creating the requests for the nonRelVal workflows
                        #    it should be also the case when we migrate to Rucio
                        ddmRequest = DDMReqTemplate('copy',
                                                    item=workflow['OutputDatasets'],
                                                    n=workflow['numberOfCopies'],
                                                    site=workflow['destination'])
                        ddmReqList.append(ddmRequest)
                except Exception as ex:
                    msg = "Could not create DDMReq for Workflow: {}".format(workflow['RequestName'])
                    msg += "Error: {}".format(ex)
                    self.logger.exception(msg)
                    return workflow

                try:
                    # In the message bellow we may want to put the list of datasets too
                    msg = "Making transfer subscriptions for {}".format(workflow['RequestName'])
                    self.logger.info(msg)
                    ddmResultList = self.ddm.makeAggRequests(ddmReqList, aggKey='item')
                except Exception as ex:
                    msg = "Could not make transfer subscription for Workflow: {}".format(workflow['RequestName'])
                    msg += "Error: {}".format(ex)
                    self.logger.exception(msg)
                    return workflow

                ddmStatusList = ['new', 'activated', 'completed', 'rejected', 'cancelled']
                transferIDs = []
                transferStatusList = []
                for ddmResult in ddmResultList:
                    if 'data' in ddmResult.keys():
                        id = deepcopy(ddmResult['data'][0]['request_id'])
                        status = deepcopy(ddmResult['data'][0]['status'])
                        transferStatusList.append({'transferID': id,
                                                   'status': status})
                        transferIDs.append(id)

                if transferStatusList and all(map(lambda x:
                                                  True if x['status'] in ddmStatusList else False,
                                                  transferStatusList)):
                    self.docKeyUpdate(workflow,
                                      transferStatus='done',
                                      transferIDs=transferIDs)
                    return workflow
                else:
                    self.docKeyUpdate(workflow,
                                      transferStatus='incomplete')
                    msg = "No data found in ddmResults for %s. Either dry run mode or " % workflow['RequestName']
                    msg += "broken transfer submission to DDM. "
                    msg += "ddmResults: \n%s" % pformat(ddmResultList)
                    self.logger.warning(msg)
                return workflow

            elif isinstance(workflow, (list, set, CommandCursor)):
                ddmRequests = {}
                for wflow in workflow:
                    wflowName = wflow['RequestName']
                    ddmRequests[wflowName] = DDMReqTemplate('copy',
                                                            item=wflow['OutputDatasets'],
                                                            n=wflow['numberOfCopies'],
                                                            site=wflow['destination'])
                if self.msConfig['enableAggSubscr']:
                    # ddmResults = self.ddm.makeAggRequests(ddmRequests.values(), aggKey='item')
                    # TODO:
                    #    Here to deal with the reverse mapping of DDM request_id to workflow
                    pass
                else:
                    # for wflowName, ddmReq in ddmRequests.items():
                    #     ddmResults.append(self.ddm.makeRequests(ddmReq))
                    # TODO:
                    #    Here to deal with making request per workflow and
                    #    reconstructing and returning the same type of object
                    #    as the one that have been passed to the current call.
                    pass
            else:
                msg = "Unsupported type %s for workflows!\n" % type(workflow)
                msg += "Skipping this call"
                self.logger.error(msg)

        elif self.msConfig['defaultDataManSys'] == 'PhEDEx':
            pass

        elif self.msConfig['defaultDataManSys'] == 'Rucio':
            pass

        # NOTE:
        #    if we are about to implement this through a pipeline we MUST not
        #    return the result here but the WHOLE document with updated fields
        #    for the transfer as it will be passed to the next function in
        #    the pipeline and uploaded to MongoDB
        return workflow

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status.
        NOTE: to be taken from MSTransferor with minor changes
        """

        # NOTE:
        #    If we are about to use an additional database for book keeping like
        #    MongoDB, we can fetch up to 'limitRequestsPerCycle' and keep track
        #    their status.

        # The following is taken from MSMonitor, just for an example.
        # get requests from ReqMgr2 data-service for given status
        # here with detail=False we get back list of records
        result = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)
        if not result:
            requests = {}
        else:
            requests = result[0]
        self.logger.info('  retrieved %s requests in status: %s', len(requests), reqStatus)

        return requests

    def msOutputConsumer(self):
        """
        A top level function to drive the creation and book keeping of all the
        subscriptions to the Data Management System
        """
        # DONE:
        #    Done: To check if the 'enableDataPlacement' flag is really taken into account
        #    Done: To make this for both relvals and non relvals
        #    Done: To return the result
        #    Done: To make report document
        #    Done: To build it through a pipe
        #    Done: To write back the updated document to MonogoDB
        msPipelineRelVal = Pipeline(name="MSOutputConsumer PipelineRelVal",
                                    funcLine=[Functor(self.docReadfromMongo,
                                                      self.msOutRelValColl,
                                                      setTaken=False),
                                              Functor(self.makeSubscriptions),
                                              Functor(self.docKeyUpdate,
                                                      isTaken=False,
                                                      isTakenBy=None,
                                                      lastUpdate=int(time())),
                                              Functor(self.docUploader,
                                                      self.msOutRelValColl,
                                                      update=True,
                                                      keys=['isTaken',
                                                            'lastUpdate',
                                                            'transferStatus',
                                                            'transferIDs']),
                                              Functor(self.docDump, pipeLine='PipelineRelVal'),
                                              Functor(self.docCleaner)])
        msPipelineNonRelVal = Pipeline(name="MSOutputConsumer PipelineNonRelVal",
                                       funcLine=[Functor(self.docReadfromMongo,
                                                         self.msOutNonRelValColl,
                                                         setTaken=False),
                                                 Functor(self.makeSubscriptions),
                                                 Functor(self.docKeyUpdate,
                                                         isTaken=False,
                                                         isTakenBy=None,
                                                         lastUpdate=int(time())),
                                                 Functor(self.docUploader,
                                                         self.msOutNonRelValColl,
                                                         update=True,
                                                         keys=['isTaken',
                                                               'lastUpdate',
                                                               'transferStatus',
                                                               'transferIDs']),
                                                 Functor(self.docDump, pipeLine='PipelineNonRelVal'),
                                                 Functor(self.docCleaner)])

        # NOTE:
        #    If we actually have any exception that has reached to the top level
        #    exception handlers (eg. here - outside the pipeLine), this means
        #    some function from within the pipeLine has not caught it and the msOutDoc
        #    has left the pipe and died before the relevant document in MongoDB
        #    has been released (its flag 'isTaken' to be set back to False)
        wfCounters = {}
        for pipeLine in [msPipelineRelVal, msPipelineNonRelVal]:
            pipeLineName = pipeLine.getPipelineName()
            wfCounters[pipeLineName] = 0
            while wfCounters[pipeLineName] < self.msConfig['limitRequestsPerCycle']:
                # take only workflows:
                # - which are not already taken or
                # - a transfer subscription have never been done for them and
                # - avoid retrying workflows in the same cycle
                # NOTE:
                #    Once we are running the service not in a dry run mode we may
                #    consider adding and $or condition in mQueryDict for transferStatus:
                #    '$or': [{'transferStatus': None},
                #            {'transferStatus': 'incomplete'}]
                #    So that we can collect also workflows with partially or fully
                #    unsuccessful transfers
                currTime = int(time())
                treshTime = currTime - self.msConfig['interval']
                mQueryDict = {
                    '$and': [
                        {'isTaken': False},
                        {'$or': [
                            {'transferStatus': None},
                            {'transferStatus': 'incomplete'}]},
                        {'$or': [
                            {'lastUpdate': None},
                            {'lastUpdate': {'$lt': treshTime}}]}]}
                try:
                    pipeLine.run(mQueryDict)
                except KeyError as ex:
                    msg = "%s Possibly malformed record in MongoDB. Err: %s. " % (pipeLineName, str(ex))
                    msg += "Continue to the next document."
                    self.logger.exception(msg)
                    continue
                except TypeError as ex:
                    msg = "%s Possibly malformed record in MongoDB. Err: %s. " % (pipeLineName, str(ex))
                    msg += "Continue to the next document."
                    self.logger.exception(msg)
                    continue
                except EmptyResultError as ex:
                    msg = "%s All relevant records in MongoDB exhausted. " % pipeLineName
                    msg += "We are done for the current cycle."
                    self.logger.info(msg)
                    break
                except Exception as ex:
                    msg = "%s General Error from pipeline. Err: %s. " % (pipeLineName, str(ex))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    self.logger.exception(ex)
                    break
                wfCounters[pipeLineName] += 1

        wfCounterTotal = sum(wfCounters.values())
        return wfCounterTotal
 
    def msOutputProducer(self, requestRecords):
        """
        A top level function to drive the upload of all the documents to MongoDB
        """

        # DONE:
        #    To implement this as a functional pipeline in the following sequence:
        #    1) document streamer - to generate all the records coming from Reqmgr2
        #    2) document stripper - to cut all the cut all the kews we do not need
        #       Mongodb document creator - to pass it through the MongoDBTemplate
        #    3) document updater - fetch & update all the needed info like campaign config etc.
        #    4) MongoDB upload/update - to upload/update the document in Mongodb

        # DONE:
        #    to have the requestRecords generated through a call to docStreamer
        #    and the call should happen from inside this function so that all
        #    the Objects generated do not leave the scope of this function and
        #    with that  to reduce big memory footprint

        # DONE:
        #    to set a destructive function at the end of the pipeline
        # NOTE:
        #    To discuss the collection names
        # NOTE:
        #    Here we should never use docUploader with `update=True`, because
        #    this will erase the latest state of already existing and fully or
        #    partially processed documents by the Consumer pipeline
        self.logger.info("Running the msOutputProducer ...")
        msPipelineRelVal = Pipeline(name="MSOutputProducer PipelineRelVal",
                                    funcLine=[Functor(self.docTransformer),
                                              Functor(self.docKeyUpdate, isRelVal=True),
                                              Functor(self.docInfoUpdate, pipeLine='PipelineRelVal'),
                                              Functor(self.docUploader, self.msOutRelValColl),
                                              Functor(self.docCleaner)])
        msPipelineNonRelVal = Pipeline(name="MSOutputProducer PipelineNonRelVal",
                                       funcLine=[Functor(self.docTransformer),
                                                 Functor(self.docKeyUpdate, isRelVal=False),
                                                 Functor(self.docInfoUpdate, pipeLine='PipelineNonRelVal'),
                                                 Functor(self.docUploader, self.msOutNonRelValColl),
                                                 Functor(self.docCleaner)])
        # TODO:
        #    To generate the object from within the Function scope see above.
        counter = 0
        for _, request in requestRecords:
            counter += 1
            try:
                if request.get('SubRequestType') == 'RelVal':
                    pipeLine = msPipelineRelVal
                    pipeLineName = pipeLine.getPipelineName()
                    pipeLine.run(request)
                else:
                    pipeLine = msPipelineNonRelVal
                    pipeLineName = pipeLine.getPipelineName()
                    pipeLine.run(request)
            except KeyError as ex:
                msg = "%s Possibly broken read from Reqmgr2 API or other Err: %s. " % (pipeLineName, str(ex))
                msg += "Continue to the next document."
                self.logger.exception(msg)
                continue
            except TypeError as ex:
                msg = "%s Possibly broken read from Reqmgr2 API or other Err: %s. " % (pipeLineName, str(ex))
                msg += "Continue to the next document."
                self.logger.exception(msg)
                continue
            except Exception as ex:
                msg = "%s General Error from pipeline. Err: %s. " % (pipeLineName, str(ex))
                msg += "Giving up Now."
                self.logger.error(msg)
                self.logger.exception(ex)
                break
        return counter

    def docTransformer(self, doc):
        """
        A function used to transform a request record from reqmgr2 to a document
        suitable for uploading to Mongodb
        """
        # Solution 1: Destructive function - to force clear of the the externally
        #             referenced object and to return a new one (current solution)
        #             NOTE: Leaves an empty dictionary behind (the clear method just
        #                   clears all the keys of the dict, but does not delete it)
        # Solution 2: To work in place (will keep the dynamic structure of the passed dict)
        # Solution 3: To have 2 object buffers for the two diff types outside the function
        try:
            msOutDoc = MSOutputTemplate(doc)
            doc.clear()
        except Exception as ex:
            msg = "ERR: Unable to create MSOutputTemplate for document %s" % pformat(doc)
            msg += "ERR: %s" % str(ex)
            self.logger.error(msg)
        return msOutDoc

    def docDump(self, msOutDoc, pipeLine=None):
        """
        Prints document contents
        """
        msg = "{}: {}: Processed 'msOutDoc' with '_id': {}.".format(self.currThreadIdent,
                                                                    pipeLine,
                                                                    msOutDoc['_id'])
        self.logger.info(msg)
        self.logger.debug(pformat(msOutDoc))
        return msOutDoc

    def docKeyUpdate(self, msOutDoc, **kwargs):
        """
        A function used to update one or few particular fields in a document
        :**kwargs: The keys/value pairs to be updated (will be tested against MSOutputTemplate)
        """
        for key, value in kwargs.items():
            try:
                msOutDoc.setKey(key, value)
                msOutDoc.updateTime()
            except Exception as ex:
                msg = "Cannot update key {} for doc: {}\n".format(key, msOutDoc['_id'])
                msg += "Error: {}".format(str(ex))
                self.logger.warning(msg)
        return msOutDoc

    def docInfoUpdate(self, msOutDoc, pipeLine=None):
        """
        A function intended to fetch and fill into the document all the needed
        additional information like campaignOutputMap etc.
        """

        # Fill the destinationOutputMap first
        if msOutDoc['isRelVal']:
            destinationOutputMap = []
            wflowDstSet = set()
            updateDict = {}
            for dataset in msOutDoc['OutputDatasets']:
                _, dsn, procString, dataTier = dataset.split('/')
                destination = set()
                if dataTier != "RECO" and dataTier != "ALCARECO":
                    destination.add('T2_CH_CERN')
                if dataTier == "GEN-SIM":
                    destination.add('T1_US_FNAL_Disk')
                if dataTier == "GEN-SIM-DIGI-RAW":
                    destination.add('T1_US_FNAL_Disk')
                if dataTier == "GEN-SIM-RECO":
                    destination.add('T1_US_FNAL_Disk')
                if "RelValTTBar" in dsn and "TkAlMinBias" in procString and dataTier != "ALCARECO":
                    destination.add('T2_CH_CERN')
                if "MinimumBias" in dsn and "SiStripCalMinBias" in procString and dataTier != "ALCARECO":
                    destination.add('T2_CH_CERN')
                dMap = {'datasets': [dataset],
                        'destination': list(destination)}
                destinationOutputMap.append(dMap)
                wflowDstSet |= destination

            # here we try to aggregate the destination map per destination
            aggDstMap = []

            # populate the first element in the aggregated list
            aggDstMap.append(destinationOutputMap.pop())

            # feed the rest
            while len(destinationOutputMap) != 0:
                dMap = destinationOutputMap.pop()
                found = False
                for aggMap in aggDstMap:
                    if set(dMap['destination']) == set(aggMap['destination']):
                        # Check if the two objects are not references to one and the
                        # same object. Only then copy the values of the dMap,
                        # otherwise we will enter an endless cycle.
                        if dMap is not aggMap:
                            for i in dMap['datasets']:
                                aggMap['datasets'].append(i)
                        found = True
                        del(dMap)
                        break
                if not found:
                    aggDstMap.append(dMap)

            # finally reassign the destination map with the aggregated one
            destinationOutputMap = aggDstMap

            wflowDstList = list(wflowDstSet)
            updateDict['destination'] = wflowDstList
            updateDict['destinationOutputMap'] = destinationOutputMap
            try:
                msOutDoc.updateDoc(updateDict, throw=True)
                # msg = "%s: %s: 'msOutDoc': %s"
                # self.logger.debug(msg,
                #                   self.currThreadIdent,
                #                   pipeLine,
                #                   pformat(msOutDoc))
            except Exception as ex:
                msg = "%s: %s: Could not update the additional information for "
                msg += "'msOutDoc' with '_id': %s \n"
                msg += "Error: %s"
                self.logger.exception(msg,
                                      self.currThreadIdent,
                                      pipeLine,
                                      msOutDoc['_id'],
                                      str(ex))
        return msOutDoc

    def docUploader(self, msOutDoc, dbColl, update=False, keys=None, stride=None):
        """
        A function to upload documents to MongoDB. The session object to the  relevant
        database and Collection must be passed as arguments
        :msOutDocs: A list of documents of type MSOutputTemplate
        :dbColl: an object containing an active connection to a MongoDB Collection
        :stride: the max number of documents we are about to upload at once
        :update: A flag to trigger document update in MongoDB in case of duplicates
        :keys:   A list of keys to update. If missing the whole document will be updated
        """
        # DONE: to determine the collection to which the document belongs based
        #       on 'isRelval' key or some other criteria
        # NOTE: We must return the document(s) at the end so that it can be explicitly
        #       deleted outside the pipeline

        # Skipping documents avoiding index unique property (documents having the
        # same value for the indexed key as an already uploaded document)
        try:
            dbColl.insert_one(msOutDoc)
        except errors.DuplicateKeyError:
            # DONE:
            #    Here we may wish to double check and make document update, so
            #    that a change of the Request on ReqMgr may be reflected here too
            # NOTE:
            #    If we use the 'update' option with a fresh document created from
            #    Reqmgr and we overwrite an already existing document in MongoDB
            #    which have been already worked on - we will loose the information
            #    that have been stored in the MonggDB - so always use 'update'
            #    with the proper set of keys to be updated
            if not keys:
                keys = []

            # update only the requested keys:
            if update and keys:
                updateDict = {}
                for key in keys:
                    updateDict[key] = msOutDoc[key]
                msOutDoc = dbColl.find_one_and_update(
                    {'_id': msOutDoc['_id']},
                    {'$set': updateDict},
                    return_document=ReturnDocument.AFTER)
            if update and not keys:
                msOutDoc = dbColl.find_one_and_update(
                    {'_id': msOutDoc['_id']},
                    {'$set': msOutDoc},
                    return_document=ReturnDocument.AFTER)
        return msOutDoc

    def docReadfromMongo(self, mQueryDict, dbColl, setTaken=False):
        """
        Reads a single Document from MongoDB and if setTaken flag is on then
        Sets the relevant flags (isTaken, isTakenBy) in the document at MongoDB
        """
        # NOTE:
        #    In case the current query returns an empty document from MongoDB
        #    (eg. all workflows have been processed) the MSOutputTemplate
        #    will throw an error. We should catch this one here and interrupt
        #    the pipeLine traversal, otherwise an error either here or in one of the
        #    following stages will most probably occur and the whole run will be broken.
        if setTaken:
            lastUpdate = int(time())
            retrString = self.currThreadIdent
            mongoDoc = dbColl.find_one_and_update(mQueryDict,
                                                  {'$set': {'isTaken': True,
                                                            'isTakenBy': retrString,
                                                            'lastUpdate': lastUpdate}},
                                                  return_document=ReturnDocument.AFTER)
        else:
            mongoDoc = dbColl.find_one(mQueryDict)
        if mongoDoc:
            try:
                msOutDoc = MSOutputTemplate(mongoDoc)
            except Exception as ex:
                msg = "Unable to create msOutDoc from %s." % mongoDoc
                self.logger.warning(msg)
                raise ex
                # NOTE:
                #    Here if we do not update the isTaken flag in MongoDB back
                #    to False, the document will not be released in MongoDb and
                #    will stay locked. If we are ending up here it means for some
                #    reason we have a malformed document in MongoDB. We should make
                #    a design choice - should we release the document or should
                #    we leave it locked for further investigations or maybe
                #    mark it with another flag eg. 'isMalformed': True
        else:
            raise EmptyResultError
        return msOutDoc

    def docCleaner(self, doc):
        """
        Calls the dictionary internal method clear() and purges all the contents
        of the document
        """
        return doc.clear()
