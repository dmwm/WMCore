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
from WMCore.Services.Rucio.Rucio import Rucio
from Utils.EmailAlert import EmailAlert
from Utils.Pipeline import Pipeline, Functor
from WMCore.Database.MongoDB import MongoDB
from WMCore.MicroService.DataStructs.MSOutputTemplate import MSOutputTemplate
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


class UnsupportedError(MSOutputException):
    """
    A MSOutputException signalling an unsupported mode for a function or method.
    """
    def __init__(self, message=None):
        if message:
            self.myMessage = "UnsupportedError: %s"
        else:
            self.myMessage = "UnsupportedError."
        super(UnsupportedError, self).__init__(self.myMessage)


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
        super(MSOutput, self).__init__(msConfig, logger=logger)

        self.mode = mode
        self.msConfig.setdefault("limitRequestsPerCycle", 500)
        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 600)
        self.msConfig.setdefault("services", ['output'])
        self.msConfig.setdefault("defaultDataManSys", "DDM")
        self.msConfig.setdefault("defaultGroup", "DataOps")
        self.msConfig.setdefault("enableAggSubscr", True)
        self.msConfig.setdefault("enableDataPlacement", False)
        self.msConfig.setdefault("excludeDataTier", ['NANOAOD', 'NANOAODSIM'])
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
        if self.msConfig['defaultDataManSys'] == 'DDM':
            self.ddm = DDM(url=self.msConfig['ddmUrl'],
                           logger=self.logger,
                           enableDataPlacement=self.msConfig['enableDataPlacement'])
        elif self.msConfig['defaultDataManSys'] == 'Rucio':
            self.rucio = Rucio(self.msConfig['rucioAccount'],
                               configDict={"logger": self.logger})

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
        #self.currHost = gethostname()
        self.currThread = current_thread()
        #self.currThreadIdent = "%s:%s@%s" % (self.currThread.name, self.currThread.ident, self.currHost)
        self.currThreadIdent = "%s" % self.currThread.name

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
        The function to update caches and to execute the Producer function itself
        """
        summary = dict(OUTPUT_PRODUCER_REPORT)
        self.updateReportDict(summary, "thread_id", self.currThreadIdent)
        msg = "{}: MSOutput is running in mode: {}".format(self.currThreadIdent, self.mode)
        self.logger.info(msg)

        try:
            requestRecords = {}
            for status in reqStatus:
                requestRecords.update(self.getRequestRecords(status))
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
            total_num_requests = self.msOutputProducer(requestRecords)
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
                    if workflow['IsRelVal']:
                        group = 'RelVal'
                    else:
                        group = 'DataOps'

                    for dMap in workflow['DestinationOutputMap']:
                        try:
                            ddmRequest = DDMReqTemplate('copy',
                                                        item=dMap['Datasets'],
                                                        n=workflow['NumberOfCopies'],
                                                        site=dMap['Destination'],
                                                        group=group)
                        except KeyError as ex:
                            # NOTE:
                            #    If we get to here it is most probably because the 'site'
                            #    mandatory field to the DDM request is missing (due to an
                            #    'ALCARECO' dataset from a Relval workflow or similar).
                            #    Since this is expected to happen a lot, we'd better just
                            #    log a warning and continue
                            msg = "Could not create DDMReq for Workflow: {}".format(workflow['RequestName'])
                            msg += "Error: {}".format(ex)
                            self.logger.warning(msg)
                            continue
                        ddmReqList.append(ddmRequest)

                except Exception as ex:
                    msg = "Could not create DDMReq for Workflow: {}".format(workflow['RequestName'])
                    msg += "Error: {}".format(ex)
                    self.logger.exception(msg)
                    return workflow

                try:
                    # In the message bellow we may want to put the list of datasets too
                    self.logger.info("Making transfer subscriptions for %s", workflow['RequestName'])

                    if ddmReqList:
                        ddmResultList = self.ddm.makeAggRequests(ddmReqList, aggKey='item')
                    else:
                        # NOTE:
                        #    Nothing else to be done here. We mark the document as
                        #    done so we do not iterate through it multiple times
                        msg = "Skip submissions for %s. Either all data Tiers were "
                        msg += "excluded or there were no Output Datasets at all. "
                        msg += "Marking this workflow as `done`."
                        self.logger.warning(msg, workflow['RequestName'])
                        self.docKeyUpdate(workflow, TransferStatus='done')
                        return workflow
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
                        transferId = deepcopy(ddmResult['data'][0]['request_id'])
                        status = deepcopy(ddmResult['data'][0]['status'])
                        transferStatusList.append({'transferID': transferId,
                                                   'status': status})
                        transferIDs.append(id)

                if transferStatusList and all(map(lambda x:
                                                  True if x['status'] in ddmStatusList else False,
                                                  transferStatusList)):
                    self.docKeyUpdate(workflow,
                                      TransferStatus='done',
                                      TransferIDs=transferIDs)
                    return workflow
                else:
                    self.docKeyUpdate(workflow,
                                      TransferStatus='pending')
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
                                                            n=wflow['NumberOfCopies'],
                                                            site=wflow['Destination'])
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
                # FIXME:
                msg = "Not yet implemented mode with workflows of type %s!\n" % type(workflow)
                msg += "Skipping this call"
                self.logger.error(msg)
                raise NotImplementedError
            else:
                msg = "Unsupported type %s for workflows!\n" % type(workflow)
                msg += "Skipping this call"
                self.logger.error(msg)
                raise UnsupportedError

        elif self.msConfig['defaultDataManSys'] == 'PhEDEx':
            pass

        elif self.msConfig['defaultDataManSys'] == 'Rucio':
            if isinstance(workflow, MSOutputTemplate):
                self.logger.info("Making transfer subscriptions for %s", workflow['RequestName'])

                rucioResultList = []
                if workflow['DestinationOutputMap']:
                    for dMap in workflow['DestinationOutputMap']:
                        try:
                            copies = workflow['NumberOfCopies']
                            # NOTE:
                            #    Once we get rid of DDM this rseExpression generation
                            #    should go in the Producer thread
                            if workflow['IsRelVal']:
                                if dMap['Destination']:
                                    rseUnion = '('+'|'.join(dMap['Destination'])+')'
                                else:
                                    # NOTE:
                                    #    If we get to here it is most probably because the destination
                                    #    in the destinationOutputMap is empty (due to an
                                    #    'ALCARECO' dataset from a Relval workflow or similar).
                                    #    Since this is expected to happen a lot, we'd better just
                                    #    log a warning and continue
                                    msg = "No destination provided. Avoid creating transfer subscription for "
                                    msg += "Workflow: %s : Dataset Names: %s"
                                    self.logger.warning(msg, workflow['RequestName'], dMap['datasets'])
                                    continue
                                rseExpression = rseUnion + '&cms_type=real&rse_type=DISK'
                                # NOTE:
                                #    The above rseExpression should resolve to something similar to:
                                #    (T2_CH_CERN|T1_US_FNAL_Disk)&cms_type=real&rse_type=DISK
                                #    where the first part is a Union of all destination sites and
                                #    the second part is a general constraint for those to be real
                                #    entries but not `Test` or `Temp` and we also target only sites
                                #    marked as `Disk`
                            else:
                                rseExpression = '(tier=2|tier=1)&cms_type=real&rse_type=DISK'
                                # NOTE:
                                #    The above rseExpression should target all T1_*_Disk and T2_*
                                #    sites, where the first part is a Union of those Tiers and
                                #    the second part is a general constraint for those to be real
                                #    entries but not `Test` or `Temp` and we also target only sites
                                #    marked as `Disk`

                            if self.msConfig['enableDataPlacement']:
                                rucioResultList.append(self.rucio.createReplicationRule(dMap['datasets'],
                                                                                        rseExpression,
                                                                                        copies=copies))
                            else:
                                msg = "DRY-RUN:: The effective Rucio submission would look like: \n"
                                msg += "account: %s \n"
                                msg += "dids: %s \n"
                                msg += "rseExpression: %s\n"
                                msg += "copies: %s\n"
                                self.logger.warning(msg,
                                                    self.msConfig['rucioAccount'],
                                                    pformat(dMap['datasets']),
                                                    rseExpression,
                                                    copies)
                        except Exception as ex:
                            msg = "Could not make transfer subscription for Workflow: %s\n:%s"
                            self.logger.exception(msg, workflow['RequestName'], str(ex))
                            return workflow
                else:
                    # NOTE:
                    #    Nothing else to be done here. We mark the document as
                    #    done so we do not iterate through it multiple times
                    msg = "Skip submissions for %s. Either all data Tiers were "
                    msg += "excluded or there were no Output Datasets at all. "
                    msg += "Marking this workflow as `done`."
                    self.logger.warning(msg, workflow['RequestName'])
                    self.docKeyUpdate(workflow, TransferStatus='done')
                    return workflow

                transferIDs = rucioResultList
                if self.msConfig['enableDataPlacement']:
                    self.docKeyUpdate(workflow,
                                      TransferStatus='done',
                                      TransferIDs=transferIDs)
                return workflow

            elif isinstance(workflow, (list, set, CommandCursor)):
                # FIXME:
                msg = "Not yet implemented mode with workflows of type %s!\n" % type(workflow)
                msg += "Skipping this call"
                self.logger.error(msg)
                raise NotImplementedError

            else:
                msg = "Unsupported type %s for workflows!\n" % type(workflow)
                msg += "Skipping this call"
                self.logger.error(msg)
                raise UnsupportedError
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
        self.logger.info("Fetching requests in status: %s", reqStatus)
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
                                    funcLine=[Functor(self.makeSubscriptions),
                                              Functor(self.docKeyUpdate,
                                                      LastUpdate=int(time())),
                                              Functor(self.docUploader,
                                                      update=True,
                                                      keys=['LastUpdate',
                                                            'TransferStatus',
                                                            'TransferIDs']),
                                              Functor(self.docDump, pipeLine='PipelineRelVal'),
                                              Functor(self.docCleaner)])
        msPipelineNonRelVal = Pipeline(name="MSOutputConsumer PipelineNonRelVal",
                                       funcLine=[Functor(self.makeSubscriptions),
                                                 Functor(self.docKeyUpdate,
                                                         LastUpdate=int(time())),
                                                 Functor(self.docUploader,
                                                         update=True,
                                                         keys=['LastUpdate',
                                                               'TransferStatus',
                                                               'TransferIDs']),
                                                 Functor(self.docDump, pipeLine='PipelineNonRelVal'),
                                                 Functor(self.docCleaner)])

        wfCounterTotal = 0
        mQueryDict = {'TransferStatus': 'pending'}
        pipeCollections = [(msPipelineRelVal, self.msOutRelValColl),
                           (msPipelineNonRelVal, self.msOutNonRelValColl)]
        for pipeColl in pipeCollections:
            wfCounters = 0
            pipeLine = pipeColl[0]
            dbColl = pipeColl[1]
            pipeLineName = pipeLine.getPipelineName()
            for docOut in self.getDocsFromMongo(mQueryDict, dbColl, self.msConfig['limitRequestsPerCycle']):
                # FIXME:
                #    To redefine those exceptions as MSoutputExceptions and
                #    start using those here so we do not mix with general errors
                try:
                    pipeLine.run(docOut)
                except (KeyError, TypeError) as ex:
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
                    msg = "%s General error from pipeline. Err: %s. " % (pipeLineName, str(ex))
                    msg += "Will retry again in the next cycle."
                    self.logger.exception(msg)
                    break
                wfCounters += 1
            self.logger.info("Processed %d workflows from pipeline: %s", wfCounters, pipeLineName)
            wfCounterTotal += wfCounters

        return wfCounterTotal

    def msOutputProducer(self, requestRecords):
        """
        A top level function to fetch requests from ReqMgr2, and produce the correspondent
        records for MSOutput in MongoDB.
        :param requestRecords: list of request dictionaries retrieved from ReqMgr2

        It's implemented as a pipeline, performing the following sequential actions:
           1) document transformer - creates a MSOutputTemplate object from the request dict
           2) document info updater - parses the MSOutputTemplate object and updates the
              necessary data structure mapping output/locations/campaign/etc
           3) document uploader - inserts the MSOutputTemplate object into the correct
              MongoDB collection (ReVal is separated from standard workflows)
           4) document cleaner - releases memory reference to the MSOutputTemplate object
        """
        # DONE:
        #    to set a destructive function at the end of the pipeline
        # NOTE:
        #    To discuss the collection names
        # NOTE:
        #    Here we should never use docUploader with `update=True`, because
        #    this will erase the latest state of already existing and fully or
        #    partially processed documents by the Consumer pipeline
        self.logger.info("Running the msOutputProducer ...")
        msPipeline = Pipeline(name="MSOutputProducer Pipeline",
                              funcLine=[Functor(self.docTransformer),
                                        Functor(self.docInfoUpdate),
                                        Functor(self.docUploader),
                                        Functor(self.docCleaner)])
        # TODO:
        #    To generate the object from within the Function scope see above.
        counter = 0
        for _, request in requestRecords.viewitems():
            counter += 1
            try:
                pipeLineName = msPipeline.getPipelineName()
                msPipeline.run(request)
            except (KeyError, TypeError) as ex:
                msg = "%s Possibly broken read from ReqMgr2 API or other. Err: %s." % (pipeLineName, str(ex))
                msg += " Continue to the next document."
                self.logger.exception(msg)
                continue
            except Exception as ex:
                msg = "%s General Error from pipeline. Err: %s. " % (pipeLineName, str(ex))
                msg += "Giving up Now."
                self.logger.exception(str(ex))
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
        except Exception as ex:
            msg = "ERR: Unable to create MSOutputTemplate for document: \n%s\n" % pformat(doc)
            msg += "ERR: %s" % str(ex)
            self.logger.exception(msg)
            raise ex
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

    def docInfoUpdate(self, msOutDoc):
        """
        A function intended to fetch and fill into the document all the needed
        additional information like campaignOutputMap etc.
        """

        # Fill the destinationOutputMap first
        destinationOutputMap = []
        wflowDstSet = set()
        updateDict = {}
        for dataset in msOutDoc['OutputDatasets']:
            _, dsn, procString, dataTier = dataset.split('/')
            # NOTE:
            #    Data tiers that have been configured to be excluded will never
            #    enter the destinationOutputMap
            if dataTier in self.msConfig['excludeDataTier']:
                # msg = "%s: %s: "
                # msg += "Data Tier: %s is blacklisted. "
                # msg += "Skipping dataset placement for: %s:%s"
                # self.logger.info(msg,
                #                  self.currThreadIdent,
                #                  pipeLine,
                #                  dataTier,
                #                  msOutDoc['RequestName'],
                #                  dataset)
                continue

            destination = set()

            if msOutDoc['IsRelVal']:
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
            else:
                # FIXME:
                #    Here we need to use the already created campaignMap for
                #    building the destinationOutputMap for nonRelVal workflows.
                #    For the time being it is a fallback to all T1_* and all T2_*.
                #    Once we migrate to Rucio we should change those defaults to
                #    whatever is the format in Rucio (eg. referring a subscription
                #    rule like: "store it at a good site" or "Store in the USA" etc.)
                destination.add('T1_*_Disk')
                destination.add('T2_*')

            dMap = {'Datasets': [dataset],
                    'Destination': list(destination)}
            destinationOutputMap.append(dMap)
            wflowDstSet |= destination

        # here we try to aggregate the destination map per destination
        aggDstMap = []

        # populate the first element in the aggregated list
        if len(destinationOutputMap) != 0:
            aggDstMap.append(destinationOutputMap.pop())

        # feed the rest
        while len(destinationOutputMap) != 0:
            dMap = destinationOutputMap.pop()
            found = False
            for aggMap in aggDstMap:
                if set(dMap['Destination']) == set(aggMap['Destination']):
                    # Check if the two objects are not references to one and the
                    # same object. Only then copy the values of the dMap,
                    # otherwise we will enter an endless cycle.
                    if dMap is not aggMap:
                        for i in dMap['Datasets']:
                            aggMap['Datasets'].append(i)
                    found = True
                    del dMap
                    break
            if not found:
                aggDstMap.append(dMap)

        # finally reassign the destination map with the aggregated one
        destinationOutputMap = aggDstMap

        wflowDstList = list(wflowDstSet)
        updateDict['Destination'] = wflowDstList
        updateDict['DestinationOutputMap'] = destinationOutputMap
        try:
            msOutDoc.updateDoc(updateDict, throw=True)
        except Exception as ex:
            msg = "%s: Could not update the additional information for "
            msg += "'msOutDoc' with '_id': %s \n"
            msg += "Error: %s"
            self.logger.exception(msg,
                                  self.currThreadIdent,
                                  msOutDoc['_id'],
                                  str(ex))
        return msOutDoc

    def docUploader(self, msOutDoc, update=False, keys=None, stride=None):
        """
        A function to upload documents to MongoDB. The session object to the  relevant
        database and Collection must be passed as arguments
        :msOutDocs: A list of documents of type MSOutputTemplate
        :stride: the max number of documents we are about to upload at once
        :update: A flag to trigger document update in MongoDB in case of duplicates
        :keys:   A list of keys to update. If missing the whole document will be updated
        """
        # NOTE: We must return the document(s) at the end so that it can be explicitly
        #       deleted outside the pipeline

        # figure out which database collection to use, based on RelVal or standard workflow
        if msOutDoc["IsRelVal"]:
            dbColl = self.msOutRelValColl
        else:
            dbColl = self.msOutNonRelValColl
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

    def getDocsFromMongo(self, mQueryDict, dbColl, limit=1000):
        """
        Reads documents from MongoDB and convert them to an MSOutputTemplate
        object. Limit can be provided to control the amount of records to be
        returned:
        :param mQueryDict: dictionary with the Mongo query to be executed
        :param dbColl: connection object to the database/collection
        :param limit: integer with the amount of documents meant to be returned
        :return: it yields an MSOutputTemplate object
        """
        # NOTE:
        # In case the current query returns an empty document from MongoDB
        # (eg. all workflows have been processed) the MSOutputTemplate
        # will throw an error. We should catch this one here and interrupt
        # the pipeLine traversal, otherwise an error either here or in one of the
        # following stages will most probably occur and the whole run will be broken.
        counter = 0
        for mongoDoc in dbColl.find(mQueryDict):
            if counter >= limit:
                return
            try:
                msOutDoc = MSOutputTemplate(mongoDoc)
                counter += 1
                yield msOutDoc
            except Exception as ex:
                msg = "Failed to create MSOutputTemplate object from mongo record: {}".format(mongoDoc)
                msg += "Error message was: {}".format(str(ex))
                self.logger.warning(msg)
                raise ex
        else:
            self.logger.info("Query: '%s' did not return any records from MongoDB", mQueryDict)

    def docCleaner(self, doc):
        """
        Calls the dictionary internal method clear() and purges all the contents
        of the document
        """
        return doc.clear()
