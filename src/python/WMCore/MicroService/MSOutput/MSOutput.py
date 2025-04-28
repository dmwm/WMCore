"""
File       : MSOutput.py

Description: MSOutput.py class provides the whole logic behind
the Output data placement in WMCore MicroServices.
"""

# system modules
import time
from pymongo import IndexModel, ReturnDocument, errors
from pprint import pformat
from threading import current_thread
from retry import retry

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import OUTPUT_REPORT
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.MicroService.Tools.Common import gigaBytes
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.DBS.DBS3Reader import getDataTiers
from Utils.Pipeline import Pipeline, Functor
from WMCore.Database.MongoDB import MongoDB
from WMCore.MicroService.MSOutput.MSOutputTemplate import MSOutputTemplate
from WMCore.MicroService.MSOutput.RelValPolicy import RelValPolicy
from WMCore.WMException import WMException


class MSOutputException(WMException):
    """
    General Exception Class for MSOutput Module in WMCore MicroServices
    """
    def __init__(self, message):
        self.myMessage = "MSOutputException: %s" % message
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
        self.msConfig.setdefault("enableDataPlacement", False)
        # Enable relval workflows to go to tape
        self.msConfig.setdefault("enableRelValCustodial", False)
        # Enable relval workflows to go to disk
        self.msConfig.setdefault("enableRelValDisk", False)
        self.msConfig.setdefault("excludeDataTier", [])
        self.msConfig.setdefault("rucioAccount", 'wmcore_transferor')
        self.msConfig.setdefault("rucioRSEAttribute", 'dm_weight')
        self.msConfig.setdefault("rucioDiskRuleWeight", 'dm_weight')
        self.msConfig.setdefault("rucioTapeExpression", 'rse_type=TAPE\cms_type=test')
        # This Disk expression wil target all real DISK T1 and T2 RSEs
        self.msConfig.setdefault("rucioDiskExpression", '(tier=2|tier=1)&cms_type=real&rse_type=DISK')
        # fetch documents created in the last 6 months (default value)
        self.msConfig.setdefault("mongoDocsCreatedSecs", 6 * 30 * 24 * 60 * 60)
        self.msConfig.setdefault("sendNotification", False)
        self.msConfig.setdefault("relvalPolicy", [])
        self.msConfig.setdefault("ruleLifetimeRelVal", [])

        self.uConfig = {}
        # service name used to route alerts via AlertManager
        self.alertServiceName = "ms-output"

        # RelVal output data placement policy from the service configuration
        self.msConfig.setdefault("dbsUrl", "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader")
        allDBSDatatiers = getDataTiers(self.msConfig['dbsUrl'])
        allDiskRSEs = self.rucio.evaluateRSEExpression("*", returnTape=False)
        self.relvalPolicy = RelValPolicy(self.msConfig['relvalPolicy'],
                                         self.msConfig['ruleLifetimeRelVal'],
                                         allDBSDatatiers, allDiskRSEs, logger=logger)

        self.cric = CRIC(logger=self.logger)
        self.uConfig = {}
        self.campaigns = {}
        self.psn2pnnMap = {}

        self.msConfig.setdefault("mongoDBRetryCount", 3)
        self.msConfig.setdefault("mongoDBReplicaSet", None)
        self.msConfig.setdefault("mongoDBPort", None)
        self.msConfig.setdefault("mockMongoDB", False)

        msOutIndex = IndexModel('RequestName', unique=True)

        # NOTE: A full set of valid database connection parameters can be found at:
        #       https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
        msOutDBConfig = {
            'database': self.msConfig['mongoDB'],
            'server': self.msConfig['mongoDBServer'],
            'replicaSet': self.msConfig['mongoDBReplicaSet'],
            'port': self.msConfig['mongoDBPort'],
            'username': self.msConfig['mongoDBUser'],
            'password': self.msConfig['mongoDBPassword'],
            'connect': True,
            'directConnection': False,
            'logger': self.logger,
            'create': True,
            'collections': [
                ('msOutRelValColl', msOutIndex),
                ('msOutNonRelValColl', msOutIndex)]}

        mongoDB = MongoDB(**msOutDBConfig)
        self.msOutDB = getattr(mongoDB, self.msConfig['mongoDB'])
        self.msOutRelValColl = self.msOutDB['msOutRelValColl']
        self.msOutNonRelValColl = self.msOutDB['msOutNonRelValColl']
        self.currThread = None
        self.currThreadIdent = None


    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the output logic, e.g.:
        * unified configuration
        """
        self.logger.info("%s: Updating local cache information.", self.currThreadIdent)

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
        Executes the whole output data placement logic. However, updating the
        local caches is a requirement to proceed with the rest of the execution.
        :return: summary report for an execution cycle
        """
        summary = dict(OUTPUT_REPORT)

        self.currThread = current_thread()
        self.currThreadIdent = self.currThread.name
        self.updateReportDict(summary, "thread_id", self.currThreadIdent)

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

        if self.mode == 'MSOutputProducer':
            self._executeProducer(reqStatus, summary)

        elif self.mode == 'MSOutputConsumer':
            self._executeConsumer(summary)

        else:
            msg = "MSOutput is running in unsupported mode: %s\n" % self.mode
            msg += "Skipping the current run!"
            self.logger.warning(msg)
            self.updateReportDict(summary, "error", msg)

        return summary

    def _executeProducer(self, reqStatus, summary):
        """
        The function to update caches and to execute the Producer function itself
        :param summary: dictionary with some high level summary for this cycle execution
        """
        msg = "{}: MSOutput is running in mode: {}".format(self.currThreadIdent, self.mode)
        self.logger.info(msg)

        try:
            mongoDocNames = self.getRecentDocNamesFromMongo()
        except Exception as err:  # general error
            mongoDocNames = []
            msg = "{}: Unknown exception while fetching documents from MongoDB. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(err))
            self.logger.exception(msg)

        try:
            requestRecords = {}
            for status in reqStatus:
                requestRecords.update(self.getRequestRecords(status))
        except Exception as err:  # general error
            msg = "{}: Unknown exception while fetching requests from ReqMgr2. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(err))
            self.logger.exception(msg)

        # filter out documents already produced
        finalRequests = []
        for reqName, reqData in requestRecords.items():
            if reqName in mongoDocNames:
                self.logger.info("Mongo document already created for %s, skipping it.", reqName)
            else:
                finalRequests.append(reqData)
        msg = "Retrieved {} recent docs from MongoDB, ".format(len(mongoDocNames))
        msg += "{} requests from ReqMgr2, and {} are new requests to be processed.".format(len(requestRecords),
                                                                                           len(finalRequests))
        self.logger.info(msg)

        try:
            total_num_requests = self.msOutputProducer(finalRequests)
            msg = "{}: Total {} requests processed from the streamer. ".format(self.currThreadIdent,
                                                                               total_num_requests)
            self.logger.info(msg)
            self.updateReportDict(summary, "total_num_requests", total_num_requests)
        except Exception as ex:
            msg = "{}: Unknown exception while running the Producer thread. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

    def _executeConsumer(self, summary):
        """
        The function to execute the Consumer function itself
        :param summary: dictionary with some high level summary for this cycle execution
        """
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

    def makeSubscriptions(self, workflow):
        """
        The common function to make the final subscriptions
        :param workflow: a MSOutputTemplate object workflow
        :return: the MSOutputTemplate object itself (with the necessary updates in place)
        """
        if not isinstance(workflow, MSOutputTemplate):
            msg = "Unsupported type object '{}' for workflows! ".format(type(workflow))
            msg += "It needs to be of type: MSOutputTemplate"
            raise UnsupportedError(msg)

        # NOTE:
        #    Here is just an example construction of the function. None of the
        #    data structures used to visualise it is correct. To Be Updated

        ruleAttrs = {'activity': 'Production Output',
                     'lifetime': self.msConfig['rulesLifetime'],
                     'account': self.msConfig['rucioAccount'],
                     'grouping': "ALL",
                     'comment': 'WMCore MSOutput output data placement'}
        # add a configurable weight value
        ruleAttrs["weight"] = self.msConfig['rucioDiskRuleWeight']

        # if anything fail along the way, set it back to "pending"
        transferStatus = "done"
        for dMap in workflow['OutputMap']:
            if dMap['Copies'] == 0:
                msg = "Output dataset configured to 0 copies, so skipping it. Details:"
                msg += "\n\tWorkflow name: {}".format(workflow['RequestName'])
                msg += "\n\tDataset name: {}".format(dMap['Dataset'])
                msg += "\n\tCampaign name: {}".format(dMap['Campaign'])
                self.logger.warning(msg)
                continue
            if dMap['DiskRuleID']:
                msg = "Output dataset: {} from workflow: {} ".format(dMap['Dataset'], workflow['RequestName'])
                msg += " has been already locked by rule id: {}".format(dMap['DiskRuleID'])
                self.logger.info(msg)
                continue

            ruleAttrs.update({'copies': dMap['Copies']})
            # RelVals have a different lifetime setting and it depends on the sample type
            if workflow['IsRelVal']:
                ruleLifeT = self.relvalPolicy.getLifetimeByDataset(dMap['Dataset'])
                ruleAttrs["lifetime"] = ruleLifeT

            msg = "Performing rucio rule creation for workflow: {}, ".format(workflow['RequestName'])
            msg += "with the following information: {}".format(dMap)
            self.logger.info(msg)

            if self.msConfig['enableDataPlacement']:
                resp = self.rucio.createReplicationRule(dMap['Dataset'], dMap['DiskDestination'], **ruleAttrs)
                if not resp:
                    # then the call failed
                    transferStatus = "pending"
                elif len(resp) == 1:
                    dMap['DiskRuleID'] = resp[0]
                elif len(resp) > 1:
                    msg = "Rule creation returned multiple rule IDs and it needs to be investigated!!! "
                    msg += "For DID: {}, rseExpr: {} and rucio account: {}".format(dMap['Dataset'],
                                                                                   dMap['DiskDestination'],
                                                                                   ruleAttrs['account'])
                    self.logger.critical(msg)
                    return workflow
            else:
                msg = "DRY-RUN RUCIO: skipping rule creation for DID: {}, ".format(dMap['Dataset'])
                msg += "rseExpr: {} and standard parameters: {}".format(dMap['DiskDestination'], ruleAttrs)
                self.logger.info(msg)

        # Finally, update the MSOutput template document with either partial or
        # complete transfer ids
        self.docKeyUpdate(workflow, OutputMap=workflow['OutputMap'])
        workflow.updateTime()
        if transferStatus == "done":
            self.logger.info("All the disk requests succeeded for: %s. Marking it as 'done'",
                             workflow['RequestName'])
            self.docKeyUpdate(workflow, TransferStatus='done')
        else:
            self.logger.info("Disk requests partially successful for: %s. Keeping it 'pending'",
                             workflow['RequestName'])

        # NOTE:
        #    if we are about to implement this through a pipeline we MUST not
        #    return the result here but the WHOLE document with updated fields
        #    for the transfer as it will be passed to the next function in
        #    the pipeline and uploaded to MongoDB
        return workflow

    def makeTapeSubscriptions(self, workflow):
        """
        Makes the output data placement to the Tape endpoints. It works either with
        PhEDEx or with Rucio, configurable. It also relies on the Unified configuration
        to decide whether a given datatier can go to tape, and where it can be auto-approved.
        :param workflow: a MSOutputTemplate object representing a workflow
        :return: the MSOutputTemplate object itself (with the necessary updates in place)
        """
        # if anything fails along the way, set it back to "pending"
        transferStatus = "done"

        # this RSE name will be used for all output datasets to be subscribed
        # within this workflow
        dataBytesForTape = self._getDataVolumeForTape(workflow)
        tapeRSE, requiresApproval = self._getTapeDestination(dataBytesForTape)
        self.logger.info("Workflow: %s, total output size: %s GB, against RSE: %s",
                         workflow['RequestName'], gigaBytes(dataBytesForTape), tapeRSE)
        for dMap in workflow['OutputMap']:
            if not self.canDatasetGoToTape(dMap, workflow):
                continue

            # this RSE name will be used for all output datasets to be subscribed
            # within this workflow
            dMap['TapeDestination'] = tapeRSE
            ruleAttrs = {'activity': 'Production Output',
                         'account': self.msConfig['rucioAccount'],
                         'copies': 1,
                         'grouping': "ALL",
                         'ask_approval': requiresApproval,
                         'comment': 'WMCore MSOutput output data placement'}
            msg = "Creating Rucio TAPE rule for container: {} and RSE: {}".format(dMap['Dataset'],
                                                                                  dMap['TapeDestination'])
            self.logger.info(msg)

            if self.msConfig['enableDataPlacement']:
                resp = self.rucio.createReplicationRule(dMap['Dataset'], dMap['TapeDestination'], **ruleAttrs)
                if not resp:
                    # then the call failed
                    transferStatus = "pending"
                elif len(resp) == 1:
                    dMap['TapeRuleID'] = resp[0]
                elif len(resp) > 1:
                    msg = "Tape rule creation returned multiple rule IDs and it needs to be investigated!!! "
                    msg += "For DID: {}, rseExpr: {} and rucio account: {}".format(dMap['Dataset'],
                                                                                   dMap['TapeDestination'],
                                                                                   ruleAttrs['account'])
                    self.logger.critical(msg)
                    return workflow
            else:
                msg = "DRY-RUN RUCIO: skipping tape rule creation for DID: {}, ".format(dMap['Dataset'])
                msg += "rseExpr: {} and standard parameters: {}".format(dMap['TapeDestination'], ruleAttrs)
                self.logger.info(msg)

        # Finally, update the MSOutput template document with either partial or
        # complete transfer ids
        self.docKeyUpdate(workflow, OutputMap=workflow['OutputMap'])
        workflow.updateTime()
        # NOTE: updating the TransferStatus at this stage is a bit trickier, we
        # cannot bypass bad disk data placements!
        if transferStatus == "done" and workflow['TransferStatus'] == "done":
            self.logger.info("All the tape requests succeeded for: %s. Marking it as 'done'",
                             workflow['RequestName'])
        elif transferStatus == "done" and workflow['TransferStatus'] == "pending":
            self.logger.info("All the tape requests succeeded for: %s, but disk ones are still pending",
                             workflow['RequestName'])
        elif transferStatus == "pending" and workflow['TransferStatus'] == "done":
            self.logger.info("Tape requests partially successful for: %s. Marking it as 'pending'",
                             workflow['RequestName'])
            self.docKeyUpdate(workflow, TransferStatus='pending')
        else:
            self.logger.info("Tape requests partially successful for: %s. Keeping it as 'pending'",
                             workflow['RequestName'])

        return workflow

    def _getTapeDestination(self, dataSize):
        """
        Depending on which Data Management system this service is configured
        to use. Run a different procedure to find out which tape endpoint to
        select as a destination for all the output datasets in a given workflow
        :param dataSize: integer with the total amount of data to be transferred, in bytes
        :return: a string with the RSE name
        """
        # This API returns a tuple with the RSE name and whether it requires approval
        return self.rucio.pickRSE(rseExpression=self.msConfig["rucioTapeExpression"],
                                  rseAttribute=self.msConfig["rucioRSEAttribute"])

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
                                              Functor(self.makeTapeSubscriptions),
                                              Functor(self.docUploader,
                                                      update=True,
                                                      keys=['LastUpdate',
                                                            'TransferStatus',
                                                            'OutputMap']),
                                              Functor(self.docDump, pipeLine='PipelineRelVal'),
                                              Functor(self.docCleaner)])
        msPipelineNonRelVal = Pipeline(name="MSOutputConsumer PipelineNonRelVal",
                                       funcLine=[Functor(self.makeSubscriptions),
                                                 Functor(self.makeTapeSubscriptions),
                                                 Functor(self.docUploader,
                                                         update=True,
                                                         keys=['LastUpdate',
                                                               'TransferStatus',
                                                               'OutputMap']),
                                                 Functor(self.docDump, pipeLine='PipelineNonRelVal'),
                                                 Functor(self.docCleaner)])

        wfCounterTotal = 0
        mQueryDict = {'TransferStatus': 'pending'}
        pipeCollections = [(msPipelineRelVal, self.msOutRelValColl),
                           (msPipelineNonRelVal, self.msOutNonRelValColl)]
        for pipeColl in pipeCollections:
            wfCounters = 0
            wfCountersOk = 0
            pipeLine = pipeColl[0]
            dbColl = pipeColl[1]
            pipeLineName = pipeLine.getPipelineName()
            for docOut in self.getDocsFromMongo(mQueryDict, dbColl, self.msConfig['limitRequestsPerCycle']):
                # FIXME:
                #    To redefine those exceptions as MSoutputExceptions and
                #    start using those here so we do not mix with general errors
                wfCounters += 1
                try:
                    pipeLine.run(docOut)
                    wfCountersOk += 1
                except (KeyError, TypeError) as ex:
                    msg = "%s Possibly malformed record in MongoDB. Err: %s. " % (pipeLineName, str(ex))
                    msg += "Continue to the next document."
                    self.logger.exception(msg)
                    continue
                except EmptyResultError:
                    msg = "%s All relevant records in MongoDB exhausted. " % pipeLineName
                    msg += "We are done for the current cycle."
                    self.logger.info(msg)
                    break
                except Exception as ex:
                    msg = "%s General error from pipeline. Err: %s. " % (pipeLineName, str(ex))
                    msg += "Will retry again in the next cycle."
                    self.logger.exception(msg)
                    workflowname = docOut.get("_id", "")
                    self.alertGenericError(self.mode, workflowname, msg, str(ex), str(docOut))
                    continue
            self.logger.info("Successfully processed %d workflows from pipeline: %s", wfCountersOk, pipeLineName)
            self.logger.info("Failed to process %d workflows from pipeline: %s", wfCounters - wfCountersOk, pipeLineName)
            wfCounterTotal += wfCountersOk

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
        counterOk = 0
        for request in requestRecords:
            pipeLineName = msPipeline.getPipelineName()
            try:
                msPipeline.run(request)
                counterOk += 1
            except (KeyError, TypeError) as ex:
                msg = "%s Possibly broken read from ReqMgr2 API or other. Err: %s." % (pipeLineName, str(ex))
                msg += " Continue to the next document."
                self.logger.exception(msg)
                continue
            except Exception as ex:
                msg = "%s General Error from pipeline. Err: %s. " % (pipeLineName, str(ex))
                msg += "Giving up Now."
                self.logger.exception(str(ex))
                workflowname = request.get("_id", "")
                self.alertGenericError(self.mode, workflowname, msg, str(ex), str(request))
                continue
        return counterOk

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
        Parses the request parameters (a mongoDB record, not yet persisted) and finds
        out what are the disk destinations and how many copies of each dataset need to
        be made.
        :param msOutDoc: a MSOutput template object
        :return: nothing, the MSOutput template record is update in memory.
        """
        self.logger.info("Producing MongoDB record for workflow: %s", msOutDoc["RequestName"])
        updatedOutputMap = []
        notFoundDIDs = []
        for dataItem in msOutDoc['OutputMap']:
            if msOutDoc['RequestType'] == "Resubmission":
                # make sure not to subscribe the same datasets multiple times, even
                # worse, to different locations! Initial workflow will take care of everything!
                dataItem['Copies'] = 0
                updatedOutputMap.append(dataItem)
                continue
            # Fetch the dataset size, even if it does not go to Disk (it might go to Tape)
            try:
                bytesSize = self._getDatasetSize(dataItem['Dataset'])
            except KeyError:
                # then this container is unknown to Rucio, bypass and make an alert
                # Error is already reported in the Rucio module, do not spam here!
                dataItem['DatasetSize'] = 0
                dataItem['Copies'] = 0
                updatedOutputMap.append(dataItem)
                notFoundDIDs.append(dataItem['Dataset'])
                continue

            dataItem['DatasetSize'] = bytesSize

            if not self.canDatasetGoToDisk(dataItem, msOutDoc['IsRelVal']):
                # nope, this dataset cannot proceed to Disk!!
                dataItem['Copies'] = 0
                updatedOutputMap.append(dataItem)
                continue

            try:
                dataItem['Copies'] = self.campaigns[dataItem['Campaign']]["MaxCopies"]
            except KeyError:
                # it can happen for RelVals, but canDatasetGoToDisk method above
                # will already take the necessary action for non existent campaign
                dataItem['Copies'] = 1

            if msOutDoc['IsRelVal']:
                destination = self.relvalPolicy.getDestinationByDataset(dataItem['Dataset'])
                if destination:
                    # ensure each RelVal destination gets a copy of the data
                    dataItem['Copies'] = len(destination)
                    dataItem['DiskDestination'] = '|'.join(destination)
                else:
                    self.logger.warning("RelVal dataset: %s without any destination", dataItem['Dataset'])
                    dataItem['Copies'] = 0
                    updatedOutputMap.append(dataItem)
                    continue
            else:
                dataItem['DiskDestination'] = self.msConfig["rucioDiskExpression"]
            updatedOutputMap.append(dataItem)

        # if there were containers not found in Rucio, create an email alert
        if notFoundDIDs:
            # only log warning msg, the previous alerts to AlertManager were too noisy
            self.logDIDNotFound(msOutDoc["RequestName"], notFoundDIDs)

        try:
            msOutDoc.updateDoc({"OutputMap": updatedOutputMap}, throw=True)
        except Exception as ex:
            msg = "%s: Could not update the additional information for "
            msg += "'msOutDoc' with '_id': %s \n"
            msg += "Error: %s"
            self.logger.exception(msg, self.currThreadIdent, msOutDoc['_id'], str(ex))
        return msOutDoc

    def _getDatasetSize(self, datasetName):
        """
        Retrieve the dataset size from the correct DM system
        This size is needed for the tape data placement
        :param datasetName: string with the dataset name
        :return: an integer with the total dataset size, in bytes
        """
        didInfo = self.rucio.getDID(datasetName)
        # let the exception be raised if we failed to calculate the dataset size
        return didInfo["bytes"]

    def canDatasetGoToDisk(self, dataItem, isRelVal=False):
        """
        This function evaluates whether a dataset can be passed to the
        Data Management system, considering the following configurations:
          1) list of blacklisted tiers in the MicroService configuration
          2) list of white listed tiers bypassing the Unified configuration
          3) list of black and white listed tiers in the Unified config
        :param dataItem: dictionary information for this dataset, from MongoDB record
        :param isRelVal: boolean flag identifying if dataset belongs to a RelVal request
        :return: True if the dataset is allowed to pass, False otherwise
        """
        # Bypass every configuration for RelVals, keep everything on disk
        # unless the disk option for this workflow is not enabled.
        if isRelVal:
            return self.msConfig['enableRelValDisk']

        dataTier = dataItem['Dataset'].split('/')[-1]
        if dataTier in self.msConfig['excludeDataTier']:
            self.logger.warning("Skipping dataset: %s because it's excluded in the MS configuration",
                                dataItem['Dataset'])
            return False

        try:
            if dataTier in self.campaigns[dataItem['Campaign']]["TiersToDM"]:
                return True
        except KeyError:
            if isRelVal:
                msg = "Campaign not found for RelVal dataset: {} ".format(dataItem['Dataset'])
                msg += "under campaign: {}. Letting it pass though...".format(dataItem['Campaign'])
                self.logger.warning(msg)
                return True
            # log and send alert via AlertManager API
            self.alertCampaignNotFound(dataItem['Campaign'], dataItem['Dataset'])
            raise

        if dataTier in self.uConfig['tiers_to_DDM']['value']:
            return True
        elif dataTier in self.uConfig['tiers_no_DDM']['value']:
            return False
        else:
            # log and send alert via AlertManager API
            self.alertDatatierNotFound(dataTier, dataItem['Dataset'], isRelVal)
            return True

    def _getDataVolumeForTape(self, workflow):
        """
        This function does a similar logic as `canDatasetGoToTape` and
        calculates the total size of all the output datasets that need
        to be pinned on tape
        :param workflow: MSOutputTemplate object retrieved from MongoDB
        :return: integer with the total size in bytes
        """
        totalSize = 0
        for dataItem in workflow['OutputMap']:
            if workflow['IsRelVal'] and not self.msConfig['enableRelValCustodial']:
                return False
            if dataItem['TapeRuleID']:
                continue
            dataTier = dataItem['Dataset'].split('/')[-1]
            if dataTier in self.msConfig['excludeDataTier']:
                continue
            elif dataTier in self.uConfig['tiers_with_no_custodial']['value']:
                continue

            # otherwise, we are about to transfer it
            totalSize += dataItem['DatasetSize']

        return totalSize

    def canDatasetGoToTape(self, dataItem, workflow):
        """
        This function evaluates whether a container can be passed to the
        Data Management system, considering the following configurations:
          1) list of tiers banned in the MicroService configuration
          2) list of tiers allowed to bypass the Unified configuration
          3) list of allowed and banned tiers in the Unified config
        :param dataItem: output map dictionary present in the MongoDB record
        :param workflow: MSOutputTemplate object retrieved from MongoDB
        :return: True if the dataset is allowed to pass, False otherwise

        NOTE: changes here should usually be applied to `_getDataVolumeForTape` too
        """
        # NOTE: Unified has a `tape_size_limit` parameter, to prevent automatic
        # tape subscription for too large samples. We are not going to implement
        # it - for the moment - at least.
        if workflow['IsRelVal'] and not self.msConfig['enableRelValCustodial']:
            return False
        if workflow['RequestType'] == "Resubmission":
            # their parent/original workflow will take care of all the data placement
            return False
        if int(dataItem['DatasetSize']) == 0:
            # container hasn't been produced in the workflow
            return False

        if dataItem['TapeRuleID']:
            msg = "Output dataset: {} from workflow: {} ".format(dataItem['Dataset'], workflow['RequestName'])
            msg += " has been already subscribed to TAPE under request id: {}".format(dataItem['TapeRuleID'])
            self.logger.info(msg)
            return False

        dataTier = dataItem['Dataset'].split('/')[-1]
        if dataTier in self.msConfig['excludeDataTier']:
            msg = "Skipping tape data placement for dataset: {} ".format(dataItem['Dataset'])
            msg += "because it's been excluded in the MS configuration."
            self.logger.warning(msg)
            return False

        if dataTier in self.uConfig['tiers_with_no_custodial']['value']:
            msg = "Skipping tape data placement for dataset: {} ".format(dataItem['Dataset'])
            msg += "because Unified configuration sets it not to go to tape."
            self.logger.warning(msg)
            return False

        # if we are here, that means the dataset can proceed to tape
        return True

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

    def getRecentDocNamesFromMongo(self):
        """
        Fetch a list of workflow names already inserted into MongoDB.
        :param dbColl: connection object to the database/collection
        :return: a flat list of workflow names
        """
        recordList = []
        # query for documents created after this timestamp
        createdAfter = int(time.time()) - self.msConfig['mongoDocsCreatedSecs']
        thisQuery = {"CreationTime": {"$gte": createdAfter}}
        projectionFields = {"RequestName": 1, "_id": 0}
        for dbColl in [self.msOutNonRelValColl, self.msOutRelValColl]:
            self.logger.info("Querying %s for docs created after timestamp: %s", dbColl.name, createdAfter)
            for mongoDoc in dbColl.find(thisQuery, projectionFields):
                recordList.append(mongoDoc["RequestName"])
        self.logger.info("Retrieved a total of %s recent requests from MongoDB", len(recordList))
        return recordList

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
                msOutDoc = MSOutputTemplate(mongoDoc, producerDoc=False)
                counter += 1
                yield msOutDoc
            except Exception as ex:
                msg = "Failed to create MSOutputTemplate object from mongo record: {}.".format(mongoDoc)
                msg += " Error message was: {}".format(str(ex))
                self.logger.exception(msg)
                raise ex
        if not counter:
            self.logger.info("%s Query: '%s' did not return any valid record from MongoDB", dbColl.name, mQueryDict)

    def getTransferInfo(self, reqName):
        """
        Searches and reads a document from MongoDB in all collections related to
        the MSOutput service. It is supposed to be called only by MSManager e.g.:
           transferDoc = self.msOutputProducer.getTransferInfo(reqName)
        And the output to be served to the REST interface, so that all request
        transfer records can be tracked.
        :param reqName: The name of the request to be searched for
        :return: a list of all msOutDocs with the record if it exists or None otherwise
        """

        mQueryDict = {'RequestName': reqName}
        result = []
        stripKeys = ['_id']

        for dbColl in [self.msOutNonRelValColl, self.msOutRelValColl]:
            doc = dbColl.find_one(mQueryDict)
            if doc:
                for key in stripKeys:
                    doc.pop(key, None)
                result.append(doc)
                break
        return result

    def docCleaner(self, doc):
        """
        Calls the dictionary internal method clear() and purges all the contents
        of the document
        """
        return doc.clear()

    def logDIDNotFound(self, wflowName, containerList):
        """
        Log a warning message for output containers not found within
        a given workflow.
        :param wflowName: string with the workflow name
        :param containerList: list of container names
        :return: none
        """
        msg = "[MSOutput] Workflow '{}' has output datasets unknown to Rucio ".format(wflowName)
        msg += "Dataset(s): {} cannot be found in Rucio. ".format(containerList)
        msg += "Thus, we are skipping these datasets from the final output "
        msg += "data placement, such that this workflow can get archived."
        self.logger.warning(msg)

    def alertCampaignNotFound(self, campaignName, containerName):
        """
        Send an alert to Prometheus for campaign not found in the database.
        :param campaignName: string with the campaign name
        :param containerName: string with the container name
        :return: none
        """
        alertName = "ms-output: Campaign not found: {}".format(campaignName)
        alertSeverity = "high"
        alertSummary = "[MSOutput] Campaign '{}' not found in central CouchDB".format(campaignName)
        alertDescription = "Dataset: {} cannot have an output transfer rule ".format(containerName)
        alertDescription += "because its campaign: {} cannot be found in central CouchDB.".format(campaignName)
        alertDescription += " In order to get output data placement working, add it ASAP please."
        self.logger.critical(alertDescription)
        if self.msConfig["sendNotification"]:
            tag = self.alertDestinationMap.get("alertCampaignNotFound", "")
            self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                           self.alertServiceName, tag=tag)

    def alertDatatierNotFound(self, datatierName, containerName, isRelVal):
        """
        Send an alert to Prometheus for datatier not found in the configuration.
        :param datatierName: string with the datatier name
        :param containerName: string with the container name
        :param isRelVal: boolean whether it's a RelVal workflow or not
        :return: none
        """
        alertName = "ms-output: Datatier not found: {}".format(datatierName)
        alertSeverity = "high"
        alertSummary = "[MSOutput] Datatier not found in the Unified configuration: {}".format(datatierName)
        alertDescription = "Dataset: {} contains a datatier: {}".format(containerName, datatierName)
        alertDescription += " not yet inserted into Unified configuration. "
        alertDescription += "Please add it ASAP. Letting it pass for now..."
        self.logger.critical(alertDescription)
        if self.msConfig["sendNotification"] and not isRelVal:
            tag = self.alertDestinationMap.get("alertDatatierNotFound", "")
            self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                           self.alertServiceName, tag=tag)

    def alertGenericError(self, caller, workflowname, msg, exMsg, document):
        """
        Send an alert to Prometheus in the case of a generic error with ms-output

        :param caller: str, indicates if the error comes from Producer or Consumer
        :param workflowname: str, representing the workflow name
        :param msg: str, context about the error
        :param exMsg: str, excetpion message
        :param document: str, serialized mongodb document
        :return: none
        """
        alertName = "ms-output: Generic MSOutput error inside {} while processing workflow '{}'".format(caller, workflowname)
        alertSeverity = "high"
        alertSummary = "[MSOutput] Generic MSOutput error inside {} while processing workflow '{}'".format(caller, workflowname)
        alertDescription = "wf: {}\n\nmsg: {}\n\nex: {}\n\n{}".format(workflowname, msg, exMsg, document)
        self.logger.error("%s\n%s\n%s", alertName, alertSummary, alertDescription)
        if self.msConfig["sendNotification"]:
            tag = self.alertDestinationMap.get("alertGenericError", "")
            self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                           self.alertServiceName, tag=tag)


