"""
File       : MSOtput.py

Description: MSOutput.py class provides the whole logic behind
the Output data placement in WMCore MicroServices.
"""

# futures
from __future__ import division, print_function

# system modules
from retry import retry
from pymongo import IndexModel, errors
from pprint import pformat

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import OUTPUT_REPORT,\
    OUTPUT_MONGO_DOC
from WMCore.MicroService.DataStructs.Workflow import Workflow
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.RequestInfo import RequestInfo
from WMCore.Services.DDM.DDM import DDM, DDMReqTemplate
from WMCore.Services.CRIC.CRIC import CRIC
from Utils.EmailAlert import EmailAlert
from Utils.Pipeline import Pipeline, Functor
from WMCore.Database.MongoDB import MongoDB
from WMCore.MicroService.DataStructs.MSOutputTemplate import MSOutputTemplate
from WMCore.MicroService.Unified.MSOutputStreamer import MSOutputStreamer


class MSOutput(MSCore):
    """
    MSOutput.py class provides the whole logic behind the Output data placement
    in MicroServices.
    """

    def __init__(self, msConfig, logger=None, mode=None):
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
        self.msConfig.setdefault("streamerBufferFile", "/tmp/msOutput/requestRecords")
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

        if self.mode == 'MSOutputProducer':
            summary = self._executeProducer(reqStatus)

        elif self.mode == 'MSOutputConsumer':
            summary = self._executeConsumer(reqStatus)

        else:
            msg = "MSOutput is running in unsupported mode: %s\n" % self.mode
            msg += "Skipping the current run!"
            self.logger.warning(msg)

        return summary

    def _executeProducer(self, reqStatus):
        """
        The function to update caches and to execute the Producer function itslef
        """
        summary = dict(OUTPUT_REPORT)
        self.logger.info("MSOutput is running in mode: %s" % self.mode)
        try:
            total_num_requests = 0
            for status in reqStatus:
                requestRecords = self.getRequestRecords(status)
                total_num_requests += len(requestRecords)
                self.updateReportDict(summary, "total_num_requests", total_num_requests)
                msg = "  retrieved {} requests in status {}.".format(
                    len(requestRecords), status)
                msg += "Service set to process up to {} requests per cycle.".format(
                    self.msConfig["limitRequestsPerCycle"])
                self.logger.info(msg)
        except Exception as err:  # general error
            msg = "Unknown exception while fetching requests from ReqMgr2. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        try:
            self.updateCaches()
        except RuntimeWarning as ex:
            msg = "All retries exhausted! Last error was: '%s'" % str(ex)
            msg += "\nRetrying to update caches again in the next cycle."
            self.logger.error(msg)
            self.updateReportDict(summary, "error", msg)
            return summary
        except Exception as ex:
            msg = "Unknown exception updating caches. Error: %s" % str(ex)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)
            return summary

        streamer = MSOutputStreamer(bufferFile=self.msConfig['streamerBufferFile'],
                                    requestRecords=requestRecords,
                                    logger=self.logger)
        self.msOutputProducer(streamer())
        return summary

    def _executeConsumer(self, reqStatus):
        """
        The function to execute the Consumer function itslef
        """

        summary = dict(OUTPUT_REPORT)
        self.logger.info("MSOutput is running in mode: %s" % self.mode)

        # this one is put here just for example.
        self.updateReportDict(summary, "ddm_request_id", 42)

        # here to call all the funciotns from bellow and at the end to call
        # makeSubscriptions with the proper subscr parameters
        # those could be determined or kept in the mongoDB
        return summary

    def makeSubscriptions(self, workflows=[]):
        """
        The common function to make the final subscriptions. It depends on the
        default Data Management System configured through msConfig. Based on that
        The relevant service wrapper is called.
        :return: A list of results from the REST interface of the DMS in question
        """

        # NOTE:
        #    Here is just an example construction of the function. None of the
        #    data structures used to visualise it is correct. To Be Updated
        results = []
        if self.msConfig['defaultDataManSys'] == 'DDM':
            # TODO: Here to put the dryrun mode: True/False
            ddm = DDM(
                url=self.msConfig['ddmUrl'],
                logger=self.logger,
                enableDataPlacement=self.msConfig['enableDataPlacement'])

            ddmReqList = []
            for workflow in workflows:
                for output in workflow['output']:
                    ddmReqList.append(DDMReqTemplate('copy', item=output))

            if self.msConfig['enableAggSubscr']:
                results = ddm.makeAggRequests(ddmReqList, aggKey='item')
            else:
                for ddmReq in ddmReqList:
                    results.append(ddm.makeRequests(ddmReqList, aggKey='item'))

        elif self.msConfig['defaultDataManSys'] == 'PhEDEx':
            pass

        elif self.msConfig['defaultDataManSys'] == 'Rucio':
            pass

        return results

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

    def msOutputProducer(self, requestRecords):
        """
        A top level function to drive the upload of all the documents to MongoDB
        """

        # TODO:
        #      To implement this as a functional pipeline the following sequence:
        #      1) document streamer - to generate all the records coming from Reqmgr2
        #      2) document stripper - to cut all the cut all the kews we do not need
        #         Mongodb document creator - to pass it through the MongoDBTemplate
        #      3) document updater - fetch & update all the needed info like campaign config etc.
        #      4) MongoDB upload/update - to upload/update the document in Mongodb

        # TODO:
        #    to have the requestRecords generated through a call to docStreamer
        #    and the call should happen from inside this function so that all
        #    the Objects generated do not leave the scope of this function and
        #    with that  to reduce big memory footprint

        # DONE:
        #    to set a destructive function at the end of the pipeline
        # NOTE:
        #    To discuss the collection names
        self.logger.info("Running the msOutputProducer ...")
        counter = 0
        msPipelineRelVal = Pipeline([Functor(self.docTransformer),
                                     Functor(self.docUpdater),
                                     Functor(self.docUploader, self.msOutRelValColl),
                                     Functor(self.docCleaner)])
        msPipelineNonRelVal = Pipeline([Functor(self.docTransformer),
                                        Functor(self.docUpdater),
                                        Functor(self.docUploader, self.msOutNonRelValColl),
                                        Functor(self.docCleaner)])
        # TODO:
        #    To generate the object from within the Function scope see above.
        # for _, request in self.docStreamer():
        for _, request in requestRecords:
            counter += 1
            if 'SubRequestType' in request.keys() and 'RelVal' in request['SubRequestType']:
                msPipelineRelVal.run(request)
            else:
                msPipelineNonRelVal.run(request)
            # if counter == stride:
            #     break

    def mongoReader(self):
        pass

    def workflowCollector(self):
        # query Reqmgr /cache - push to mongo
        pass

    def workflowFinder(self):
        # searches and returns a workflow or a an aggregation of or flows to work on
        pass

    def _updateMongo(self):
        pass

    def _insertMongo(self):
        # Measure time and log queries
        pass

    def _pushToMongo(self, mongoDBTemplate=None):
        """
        An auxiliary function to push documents with workflow/request
        representation into mongoDB

        mongoDBTemplate: Is not the right parameter for that function
        """
        if mongoDBTemplate is None:
            mongoDBTemplate = self.mongoDBTemplate

    def docStreamer(self):
        """
        A simple representation of a document streamer
        """
        # TODO:
        #    To implement streaming in strides - chunks of documents of size 'stride'
        requests = self.getRequestRecords()
        while requests:
            yield requests.popitem()

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

    def docUpdater(self, msOutDoc):
        """
        A function intended to fetch and fill into the document all the needed
        additional information like campaignOutputMap etc.
        """
        return msOutDoc

    def docUploader(self, msOutDoc, dbColl, stride=None):
        """
        A function to upload documents to MongoDB. The session object to the  relevant
        database and Collection must be passed as arguments
        :msOutDocs: A list of documents of type MSOutputTemplate
        :dbColl: an object containing an active connection to a MongoDB Collection
        :stride: the max number of documents we are about to upload at once
        """
        # DONE: to determine the collection to which the document belongs based
        #       on 'isRelval' key or some other criteria
        # NOTE: We must return the document(s) at the end so that it can be explicitly
        #       deleted outside the pipeline

        # Skipping documents avoiding index unique property (documents having the
        # same value for the indexed key as an already uploaded document)
        try:
            dbColl.insert_one(msOutDoc)
        except errors.DuplicateKeyError as ex:
            # TODO: Here we may wish to double check and make document update, so
            #       that a change of the Request on ReqMgr may be reflected here too
            pass
        return msOutDoc

    def docCleaner(self, doc):
        return doc.clear()
