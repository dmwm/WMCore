"""
File       : MSOtput.py

Description: MSOutput.py class provides the whole logic behind
the Output data placement in WMCore MicroServices.
"""

# futures
from __future__ import division, print_function

# system modules
from http.client import HTTPException
from pymongo import IndexModel, ReturnDocument, errors
from pprint import pformat
from threading import current_thread
from retry import retry

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import OUTPUT_REPORT
from WMCore.MicroService.Unified.MSCore import MSCore
from WMCore.MicroService.Unified.Common import teraBytes, gigaBytes
from WMCore.Services.DDM.DDM import DDM, DDMReqTemplate
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.Rucio.Rucio import weightedChoice
from Utils.EmailAlert import EmailAlert
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
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

    def __init__(self, msConfig, mode, reqCache, logger=None):
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
        self.msConfig.setdefault("defaultGroup", "DataOps")
        self.msConfig.setdefault("enableDataPlacement", False)
        self.msConfig.setdefault("enableRelValCustodial", False)
        self.msConfig.setdefault("excludeDataTier", ['NANOAOD', 'NANOAODSIM'])
        self.msConfig.setdefault("rucioAccount", 'wmcore_transferor')
        self.msConfig.setdefault("rucioRSEAttribute", 'ddm_quota')
        self.msConfig.setdefault("rucioTapeExpression", 'rse_type=TAPE\cms_type=test')
        self.msConfig.setdefault("mongoDBUrl", 'mongodb://localhost')
        self.msConfig.setdefault("mongoDBPort", 8230)
        self.msConfig.setdefault("sendNotification", False)
        self.uConfig = {}
        self.emailAlert = EmailAlert(self.msConfig)

        self.cric = CRIC(logger=self.logger)
        self.uConfig = {}
        self.campaigns = {}
        self.psn2pnnMap = {}
        # cache to store request names shared between the Producer and Consumer threads
        self.requestNamesCached = reqCache

        self.tapeStatus = dict()
        for endpoint, quota in self.msConfig['tapePledges'].viewitems():
            self.tapeStatus[endpoint] = dict(quota=quota, usage=0, remaining=0)

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
        if not self.msConfig.get('useRucio', False):
            self.ddm = DDM(url=self.msConfig['ddmUrl'],
                           logger=self.logger,
                           enableDataPlacement=self.msConfig['enableDataPlacement'])


    @retry(tries=3, delay=2, jitter=2)
    def updateCaches(self):
        """
        Fetch some data required for the output logic, e.g.:
        * unified configuration
        """
        self.logger.info("%s: Updating local cache information.", self.currThreadIdent)
        self.logger.info("%s: Request names cache size: %s", self.currThreadIdent, len(self.requestNamesCached))

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
            requestRecords = {}
            for status in reqStatus:
                requestRecords.update(self.getRequestRecords(status))
        except Exception as err:  # general error
            msg = "{}: Unknown exception while fetching requests from ReqMgr2. ".format(self.currThreadIdent)
            msg += "Error: {}".format(str(err))
            self.logger.exception(msg)

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
        The common function to make the final subscriptions. It depends on the
        default Data Management System configured through msConfig. Based on that
        The relevant service wrapper is called.
        :return: the MSOutputTemplate object itself (with the necessary updates in place)
        """
        if not isinstance(workflow, MSOutputTemplate):
            msg = "Unsupported type object '{}' for workflows! ".format(type(workflow))
            msg += "It needs to be of type: MSOutputTemplate"
            raise UnsupportedError(msg)

        # NOTE:
        #    Here is just an example construction of the function. None of the
        #    data structures used to visualise it is correct. To Be Updated

        # if anything fail along the way, set it back to "pending"
        transferStatus = "done"
        if not self.msConfig.get('useRucio', False):
            # NOTE:
            #    Once we move to working in strides of multiple workflows at a time
            #    then the workflow sent to that function should not be a single one
            #    but an iterator of length 'stride' and then we should be doing:
            #    for workflow in workflows:
            if workflow['IsRelVal']:
                group = 'RelVal'
            else:
                group = 'DataOps'

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
                    msg += " has been already subscribed under request id: {}".format(dMap['DiskRuleID'])
                    self.logger.info(msg)
                    continue

                # overwrite the current DiskDestination by something that will work for DDM
                if workflow['IsRelVal']:
                    # destination is currently expressed as an RSE expression, convert it to a list
                    destination = dMap['DiskDestination'].split("|")
                else:
                    destination = ['T1_*_Disk', 'T2_*']

                # NOTE: both "site" and "item" are meant to be lists here
                ddmRequest = DDMReqTemplate('copy',
                                            item=[dMap['Dataset']],
                                            n=dMap['Copies'],
                                            site=destination,
                                            group=group)

                self.logger.info("Performing DDM subscription for workflow: %s, dataset: %s",
                                 workflow['RequestName'], dMap['Dataset'])
                resp = self.ddm.makeRequest(ddmRequest)
                if not resp:
                    # then the call failed
                    transferStatus = "pending"
                elif resp is ddmRequest:
                    msg = "DRY-RUN DDM: skipping subscription for: {}".format(ddmRequest)
                    self.logger.info(msg)
                elif 'data' in resp:
                    self.logger.debug("DDM response: %s", resp)
                    transferId = resp['data'][0]['request_id']
                    dMap['DiskRuleID'] = str(transferId)
                else:
                    self.logger.error("Something seriously BAD happened with the DDM request!")
                    return workflow

        elif self.msConfig.get('useRucio', False):
            ruleAttrs = {'activity': 'Production Output',
                         'lifetime': self.msConfig['rulesLifetime'],
                         'account': self.msConfig['rucioAccount'],
                         'grouping': "ALL",
                         'comment': 'WMCore MSOutput output data placement'}
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

                self.logger.info("Performing rucio rule creation for workflow: %s, dataset: %s",
                                 workflow['RequestName'], dMap['Dataset'])

                ruleAttrs.update({'copies': dMap['Copies']})
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
            requestOnly = "y" if requiresApproval else "n"
            if not self.msConfig.get('useRucio', False):
                phedexGroup = "RelVal" if workflow['IsRelVal'] else "DataOps"
                subscription = PhEDExSubscription(datasetPathList=dMap['Dataset'],
                                                  nodeList=dMap['TapeDestination'],
                                                  group=phedexGroup,
                                                  level="dataset",
                                                  custodial="y",
                                                  priority="low",
                                                  request_only=requestOnly,
                                                  comments="WMCore MSOutput output data placement")
                msg = "Creating PhEDEx TAPE subscription for dataset: {}, ".format(dMap['Dataset'])
                msg += "group: {} and RSE: {}".format(phedexGroup, dMap['TapeDestination'])
                self.logger.info(msg)
                if self.msConfig['enableDataPlacement']:
                    try:
                        resp = self.phedex.subscribe(subscription)
                    except HTTPException as ex:
                        msg = "Subscription failed for workflow: {} and dataset: {}".format(workflow["RequestName"],
                                                                                            dMap['Dataset'])
                        if ex.status == 400 and "request matched no data in TMDB" in ex.reason:
                            msg += " because data cannot be found in TMDB. Bypassing this dataset..."
                            self.logger.warning(msg)
                        else:
                            msg += " with status code: {} and result: {}. ".format(ex.status, ex.result)
                            msg += "It will be retried in the next cycle"
                            self.logger.error(msg)
                            transferStatus = "pending"
                    except Exception as ex:
                        msg = "Unknown exception while subscribing data for workflow: {} ".format(workflow["RequestName"])
                        msg += "and dataset: {}. Will retry again later. Error details: {}".format(dMap['Dataset'], str(ex))
                        self.logger.exception(msg)
                        transferStatus = "pending"
                    else:
                        dMap['TapeRuleID'] = resp['phedex']['request_created'][0]['id']
                else:
                    msg = "DRY-RUN PHEDEX: skipping tape subscription for: {}".format(subscription)
                    self.logger.info(msg)
            ####### then it's Rucio
            else:
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
        if self.msConfig.get('useRucio'):
            # This API returns a tuple with the RSE name and whether it requires approval
            return self.rucio.pickRSE(rseExpression=self.msConfig["rucioTapeExpression"],
                                      rseAttribute=self.msConfig["rucioRSEAttribute"],
                                      minNeeded=dataSize)

        # well, then it's PhEDEx
        res = self.phedex.getGroupUsage(node=self.tapeStatus.keys(), group=self.msConfig['defaultGroup'])
        if not res['phedex']['node']:
            raise MSOutputException("Failed to fetch node/group usage from PhEDEx")

        pnnsWithWeights = []
        self.logger.info("Summary of the current MSS situation (in Terabytes):")
        for item in res['phedex']['node']:
            self.tapeStatus[item['name']]['usage'] = teraBytes(item['group'][0]['dest_bytes'])
            self.tapeStatus[item['name']]['remaining'] = self.tapeStatus[item['name']]['quota'] - \
                                                         self.tapeStatus[item['name']]['usage']
            self.logger.info("\tPNN: %s\t\t%s", item['name'], self.tapeStatus[item['name']])
            # drop Tape endpoints that cannot host all that data
            if self.tapeStatus[item['name']]['remaining'] > teraBytes(dataSize):
                pnnsWithWeights.append((item['name'], self.tapeStatus[item['name']]['remaining']))
        winnerPnn = weightedChoice(pnnsWithWeights)
        requiresApproval = True if winnerPnn not in self.uConfig['sites_auto_approve']['value'] else False
        return winnerPnn, requiresApproval

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
            pipeLine = pipeColl[0]
            dbColl = pipeColl[1]
            pipeLineName = pipeLine.getPipelineName()
            for docOut in self.getDocsFromMongo(mQueryDict, dbColl, self.msConfig['limitRequestsPerCycle']):
                # FIXME:
                #    To redefine those exceptions as MSoutputExceptions and
                #    start using those here so we do not mix with general errors
                try:
                    # If it's in MongoDB, it can get into our in-memory cache
                    self.requestNamesCached.append(docOut['RequestName'])
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
            if request['RequestName'] in self.requestNamesCached:
                # if it's cached, then it's already in MongoDB, no need to redo this thing!
                continue
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
        Parses the request parameters (a mongoDB record, not yet persisted) and finds
        out what are the disk destinations and how many copies of each dataset need to
        be made.
        :param msOutDoc: a MSOutput template object
        :return: nothing, the MSOutput template record is update in memory.
        """
        self.logger.info("Producing MongoDB record for workflow: %s", msOutDoc["RequestName"])
        updatedOutputMap = []
        for dataItem in msOutDoc['OutputMap']:
            if msOutDoc['RequestType'] == "Resubmission":
                # make sure not to subscribe the same datasets multiple times, even
                # worse, to different locations! Initial workflow will take care of everything!
                dataItem['Copies'] = 0
                updatedOutputMap.append(dataItem)
                continue
            ### Fetch the dataset size, even if it does not go to Disk (it might go to Tape)
            bytesSize = self._getDatasetSize(dataItem['Dataset'])
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
                _, dsn, procString, dataTier = dataItem['Dataset'].split('/')
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

                if destination:
                    dataItem['DiskDestination'] = '|'.join(destination)
                else:
                    self.logger.warning("RelVal dataset: %s without any destination", dataItem['Dataset'])
                    dataItem['Copies'] = 0
                    updatedOutputMap.append(dataItem)
                    continue
            else:
                # FIXME:
                #    Here we need to use the already created campaignMap for
                #    building the destinationOutputMap for nonRelVal workflows.
                #    For the time being it is a fallback to all T1_* and all T2_*.
                #    Once we migrate to Rucio we should change those defaults to
                #    whatever is the format in Rucio (eg. referring a subscription
                #    rule like: "store it at a good site" or "Store in the USA" etc.)

                # NOTE: This default rseExpression should target all T1_*_Disk and T2_*
                # sites, where the first part is a Union of those Tiers and the second
                # part is a general constraint for those to be real entries (not `Test`
                # or `Temp`) and we also target only Disk endpoints
                dataItem['DiskDestination'] = '(tier=2|tier=1)&cms_type=real&rse_type=DISK'
            updatedOutputMap.append(dataItem)

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
        if self.msConfig.get('useRucio'):
            didInfo = self.rucio.getDID(datasetName)
            # let the exception be raised if we failed to calculate the dataset size
            return didInfo["bytes"]

        # then it's PhEDEx
        didInfo = self.phedex.getReplicaInfoForBlocks(dataset=datasetName)
        bytesSize = 0
        for item in didInfo['phedex']['block']:
            bytesSize += item['bytes']
        return bytesSize

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
            emailSubject = "[MSOutput] Campaign '{}' not found in central CouchDB".format(dataItem['Campaign'])
            emailMsg = "Dataset: {} cannot have an output transfer rule ".format(dataItem['Dataset'])
            emailMsg += "because its campaign: {} cannot be found in central CouchDB.".format(dataItem['Campaign'])
            emailMsg += " In order to get output data placement working, add it ASAP please."
            self.logger.critical(emailMsg)
            if self.msConfig["sendNotification"]:
                self.emailAlert.send(emailSubject, emailMsg)
            raise

        if dataTier in self.uConfig['tiers_to_DDM']['value']:
            return True
        elif dataTier in self.uConfig['tiers_no_DDM']['value']:
            return False
        else:
            emailSubject = "[MSOutput] Datatier not found in the Unified configuration: {}".format(dataTier)
            emailMsg = "Dataset: {} contains a datatier: {}".format(dataItem['Dataset'], dataTier)
            emailMsg += " not yet inserted into Unified configuration. "
            emailMsg += "Please add it ASAP. Letting it pass for now..."
            self.logger.critical(emailMsg)
            if self.msConfig["sendNotification"] and not isRelVal:
                self.emailAlert.send(emailSubject, emailMsg)
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
        This function evaluates whether a dataset can be passed to the
        Data Management system, considering the following configurations:
          1) list of blacklisted tiers in the MicroService configuration
          2) list of white listed tiers bypassing the Unified configuration
          3) list of black and white listed tiers in the Unified config
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
        else:
            self.logger.info("Query: '%s' did not return any records from MongoDB", mQueryDict)

    def docCleaner(self, doc):
        """
        Calls the dictionary internal method clear() and purges all the contents
        of the document
        """
        return doc.clear()
