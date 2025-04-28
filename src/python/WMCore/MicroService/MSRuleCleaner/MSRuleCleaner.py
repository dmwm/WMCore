"""
File       : MSRuleCleaner.py

Description:
This MicroService is meant to remove Rucio rules that are no longer needed in the Workload Management system, such as:
 * block-level     rules created by WMAgent, against the origin RSE where data is getting produced
 * container-level rules created by WMAgent, with a very generic RSE expression
 * block-level     rules created by MSTransferor, for input data that is no longer in the system
 * container-level rules created by MSTransferor, for input data that is no longer in the system
In addition to that, this MicroService is now also responsible for the workflow archival, which is the final status that
workflows remain.
"""
# TODO: To Add the top level description of the logic into the module's Docstring

# futures
from __future__ import division, print_function
from future.utils import viewvalues

import json
import re
import time

# system modules
from threading import current_thread
from pprint import pformat

# WMCore modules
from WMCore.MicroService.DataStructs.DefaultStructs import RULECLEANER_REPORT
from WMCore.MicroService.MSRuleCleaner.MSRuleCleanerWflow import MSRuleCleanerWflow
from WMCore.MicroService.MSCore.MSCore import MSCore
from WMCore.Services.pycurl_manager import RequestHandler
from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException
from WMCore.ReqMgr.DataStructs import RequestStatus
from WMCore.WMException import WMException
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.Services.WMStatsServer.WMStatsServer import WMStatsServer
from WMCore.MicroService.Tools.Common import findParent
from Utils.Pipeline import Pipeline, Functor
from Utils.CertTools import ckey, cert


class MSRuleCleanerResolveParentError(WMException):
    """
    WMCore exception class for the MSRuleCleaner module, in WMCore MicroServices,
    used to signal if an error occurred during parent dataset resolution step.
    """
    def __init__(self, message):
        super(MSRuleCleanerResolveParentError, self).__init__(message)


class MSRuleCleanerArchivalError(WMException):
    """
    Archival Exception Class for MSRuleCleaner Module in WMCore MicroServices
    used to signal an abnormal condition if occurred during the archival step.
    """
    def __init__(self, message):
        super(MSRuleCleanerArchivalError, self).__init__(message)


class MSRuleCleanerArchivalSkip(WMException):
    """
    Archival Exception Class for MSRuleCleaner Module in WMCore MicroServices
    used to signal an expected condition which should interrupt the archival process
    """
    def __init__(self, message):
        super(MSRuleCleanerArchivalSkip, self).__init__(message)


class MSRuleCleaner(MSCore):
    """
    MSRuleCleaner.py class provides the logic used to clean the Rucio
    block level data placement rules created by WMAgent.
    """

    def __init__(self, msConfig, logger=None):
        """
        Runs the basic setup and initialization for the MSRuleCleaner module
        :param msConfig: micro service configuration
        """
        super(MSRuleCleaner, self).__init__(msConfig, logger=logger)

        self.msConfig.setdefault("verbose", True)
        self.msConfig.setdefault("interval", 60)
        self.msConfig.setdefault("services", ['ruleCleaner'])
        self.msConfig.setdefault("rucioWmaAccount", "wma_test")
        self.msConfig.setdefault("rucioMStrAccount", "wmcore_transferor")
        self.msConfig.setdefault('enableRealMode', False)
        self.msConfig.setdefault('archiveDelayHours', 24 * 2)
        self.msConfig.setdefault('archiveAlarmHours', 24 * 30)
        self.msConfig.setdefault("sendNotification", False)

        self.currThread = None
        self.currThreadIdent = None

        self.mode = "RealMode" if self.msConfig['enableRealMode'] else "DryRunMode"
        self.curlMgr = RequestHandler()
        self.targetStatusRegex = re.compile(r'.*archived')
        self.logDB = LogDB(self.msConfig["logDBUrl"],
                           self.msConfig["logDBReporter"],
                           logger=self.logger)
        self.wmstatsSvc = WMStatsServer(self.msConfig['wmstatsUrl'], logger=self.logger)
        # service name used to route alerts via AlertManager
        self.alertServiceName = "ms-rulecleaner"
        # 2 days of expiration time
        self.alertExpiration = self.msConfig.get("alertExpireSecs", 2 * 24 * 60 * 60)

        # Building all the Pipelines:
        pName = 'plineMSTrCont'
        self.plineMSTrCont = Pipeline(name=pName,
                                      funcLine=[Functor(self.setPlineMarker, pName),
                                                Functor(self.setParentDatasets),
                                                Functor(self.getRucioRules, 'container', self.msConfig['rucioMStrAccount']),
                                                Functor(self.cleanRucioRules)])
        pName = 'plineMSTrBlock'
        self.plineMSTrBlock = Pipeline(name=pName,
                                       funcLine=[Functor(self.setPlineMarker, pName),
                                                 Functor(self.setParentDatasets),
                                                 Functor(self.getRucioRules, 'block', self.msConfig['rucioMStrAccount']),
                                                 Functor(self.cleanRucioRules)])
        pName = 'plineAgentCont'
        self.plineAgentCont = Pipeline(name=pName,
                                       funcLine=[Functor(self.setPlineMarker, pName),
                                                 Functor(self.getRucioRules, 'container', self.msConfig['rucioWmaAccount']),
                                                 Functor(self.cleanRucioRules)])
        pName = 'plineAgentBlock'
        self.plineAgentBlock = Pipeline(name=pName,
                                        funcLine=[Functor(self.setPlineMarker, pName),
                                                  Functor(self.getRucioRules, 'block', self.msConfig['rucioWmaAccount']),
                                                  Functor(self.cleanRucioRules)])
        pName = 'plineArchive'
        self.plineArchive = Pipeline(name=pName,
                                     funcLine=[Functor(self.setPlineMarker, pName),
                                               Functor(self.findTargetStatus),
                                               Functor(self.setClean),
                                               Functor(self.setArchivalDelayExpired),
                                               Functor(self.setLogDBClean),
                                               Functor(self.archive)])

        # Building the different set of plines we will need later:
        # NOTE: The following are all the functional pipelines which are supposed to include
        #       a cleanup function and report cleanup status in the MSRuleCleanerWflow object
        self.cleanuplines = [self.plineMSTrCont,
                             self.plineMSTrBlock,
                             self.plineAgentCont,
                             self.plineAgentBlock]
        # Building an auxiliary list of cleanup pipeline names only:
        self.cleanupPipeNames = [pline.name for pline in self.cleanuplines]

        # Building lists of pipelines related only to Agents or MStransferror
        self.agentlines = [self.plineAgentCont,
                           self.plineAgentBlock]
        self.mstrlines = [self.plineMSTrCont,
                          self.plineMSTrBlock]

        # Initialization of the 'cleaned' and 'archived' counters:
        self.wfCounters = {'cleaned': {},
                           'archived': {'normalArchived': 0,
                                        'forceArchived': 0}}
        self.globalLocks = set()

    def getGlobalLocks(self):
        """
        Fetches the list of 'globalLocks' from wmstats server and the list of
        'parentLocks' from request manager. Stores/updates the unified set in
        the 'globalLocks' instance variable. Returns the resultant unified set.
        :return: A union set of the 'globalLocks' and the 'parentLocks' lists
        """
        self.logger.info("Fetching globalLocks list from wmstats server.")
        try:
            globalLocks = set(self.wmstatsSvc.getGlobalLocks())
        except Exception as ex:
            msg = "Failed to refresh global locks list for the current polling cycle. Error: %s "
            msg += "Skipping this polling cycle."
            self.logger.error(msg, str(ex))
            raise ex
        self.logger.info("Fetching parentLocks list from reqmgr2 server.")
        try:
            parentLocks = set(self.reqmgr2.getParentLocks())
        except Exception as ex:
            msg = "Failed to refresh parent locks list for the current poling cycle. Error: %s "
            msg += "Skipping this polling cycle."
            self.logger.error(msg, str(ex))
            raise ex
        self.globalLocks = globalLocks | parentLocks

    def resetCounters(self):
        """
        A simple function for zeroing the cleaned and archived counters.
        """
        for pline in self.cleanuplines:
            self.wfCounters['cleaned'][pline.name] = 0
        self.wfCounters['archived']['normalArchived'] = 0
        self.wfCounters['archived']['forceArchived'] = 0

    def execute(self, reqStatus):
        """
        Executes the whole ruleCleaner logic
        :return: summary
        """
        # start threads in MSManager which should call this method
        summary = dict(RULECLEANER_REPORT)

        self.currThread = current_thread()
        self.currThreadIdent = self.currThread.name
        self.updateReportDict(summary, "thread_id", self.currThreadIdent)
        self.resetCounters()
        self.logger.info("MSRuleCleaner is running in mode: %s.", self.mode)

        # Build the list of workflows to work on:
        try:
            requestRecords = {}
            for status in reqStatus:
                requestRecords.update(self.getRequestRecords(status))
        except Exception as err:  # general error
            msg = "Unknown exception while fetching requests from ReqMgr2. Error: %s", str(err)
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        # Call _execute() and feed the relevant pipeline with the objects popped from requestRecords
        try:
            self.getGlobalLocks()
            totalNumRequests, cleanNumRequests, normalArchivedNumRequests, forceArchivedNumRequests = self._execute(requestRecords)
            msg = "\nNumber of processed workflows: %s."
            msg += "\nNumber of properly cleaned workflows: %s."
            msg += "\nNumber of normally archived workflows: %s."
            msg += "\nNumber of force archived workflows: %s."
            self.logger.info(msg,
                             totalNumRequests,
                             cleanNumRequests,
                             normalArchivedNumRequests,
                             forceArchivedNumRequests)
            self.updateReportDict(summary, "total_num_requests", totalNumRequests)
            self.updateReportDict(summary, "clean_num_requests", cleanNumRequests)
            self.updateReportDict(summary, "normal_archived_num_requests", normalArchivedNumRequests)
            self.updateReportDict(summary, "force_archived_num_requests", forceArchivedNumRequests)
        except Exception as ex:
            msg = "Unknown exception while running MSRuleCleaner thread Error: {}".format(str(ex))
            self.logger.exception(msg)
            self.updateReportDict(summary, "error", msg)

        return summary

    def _execute(self, reqRecords):
        """
        Executes the MSRuleCleaner pipelines based on the workflow status
        :param reqList: A list of RequestRecords to work on
        :return:        a tuple with:
                            number of properly cleaned requests
                            number of processed workflows
                            number of archived workflows
                            number of forced archived workflows
        """
        # NOTE: The Input Cleanup, the Block Level Cleanup and the Archival
        #       Pipelines are executed sequentially in the above order.
        #       This way we assure ourselves that we archive only workflows
        #       that have accomplished the needed cleanup

        cleanNumRequests = 0
        totalNumRequests = 0

        # Call the workflow dispatcher:
        for req in viewvalues(reqRecords):
            wflow = MSRuleCleanerWflow(req)
            self._dispatchWflow(wflow)
            msg = "\n----------------------------------------------------------"
            msg += "\nMSRuleCleanerWflow: %s"
            msg += "\n----------------------------------------------------------"
            self.logger.debug(msg, pformat(wflow))
            totalNumRequests += 1
            if self._checkClean(wflow):
                cleanNumRequests += 1

        # Report the counters:
        for pline in self.cleanuplines:
            msg = "Workflows cleaned by pipeline: %s: %d"
            self.logger.info(msg, pline.name, self.wfCounters['cleaned'][pline.name])
        normalArchivedNumRequests = self.wfCounters['archived']['normalArchived']
        forceArchivedNumRequests = self.wfCounters['archived']['forceArchived']
        self.logger.info("Workflows normally archived: %d", self.wfCounters['archived']['normalArchived'])
        self.logger.info("Workflows force archived: %d", self.wfCounters['archived']['forceArchived'])
        return totalNumRequests, cleanNumRequests, normalArchivedNumRequests, forceArchivedNumRequests

    def _dispatchWflow(self, wflow):
        """
        A function intended to dispatch a workflow (e.g based on its status)
        through one or more functional pipelines in case there is some more
        complicated logic involved in the order we execute them but not just
        a sequentially
        """
        self.logger.debug("Dispatching workflow: %s", wflow['RequestName'])
        # NOTE: The following dispatch logic is a subject to be changed at any time

        # Resolve:
        # NOTE: First resolve any preliminary flags that will be needed further
        #       in the logic of the _dispatcher() itself
        if wflow['RequestStatus'] == 'announced':
            self.getMSOutputTransferInfo(wflow)

        # Clean:
        # Do not clean any Resubmission, but still let them be archived
        if wflow['RequestType'] == 'Resubmission':
            wflow['ForceArchive'] = True
            msg = "Skipping cleanup step for workflow: %s - RequestType is %s."
            msg += " Will try to archive it directly."
            self.logger.info(msg, wflow['RequestName'], wflow['RequestType'])
        elif wflow['RequestStatus'] == 'announced' and not wflow['TransferDone']:
            # NOTE: We skip workflows which have not yet finalised their TransferStatus
            #       in MSOutput, but we still need some proper logging for them.
            msg = "Skipping workflow: %s - 'TransferStatus' is 'pending' or 'TransferInfo' is missing in MSOutput." % wflow['RequestName']
            msg += " Will retry again in the next cycle."
            self.logger.info(msg)
            self._checkStatusAdvanceExpired(wflow, additionalInfo=msg)
        elif wflow['RequestStatus'] == 'announced' and not wflow['TransferTape']:
            # Given that we are still waiting for the tape transfers to be fulfilled,
            # we can go ahead and start cleaning up the input data.
            msg = "Skipping workflow: %s - tape transfers are not yet completed." % wflow['RequestName']
            msg += "Workflow in 'announced' state, hence proceeding only with MSTransferor / input data removal. "  
            msg += "Will retry the remaining in the next cycle."
            self.logger.info(msg)
            self._checkStatusAdvanceExpired(wflow, additionalInfo=msg)
            for pline in self.mstrlines:
                try: 
                    pline.run(wflow)
                except Exception as ex:
                    msg = f"{pline.name}: General error from pipeline"
                    msg += " when cleaning input MSTransferor rules."
                    msg += f"\nWorkflow: {wflow['RequestName']}. Error:  \n{str(ex)}."
                    msg += "\nWill retry again in the next cycle."
                    self.logger.exception(msg)
                    continue
            # return now to avoid exeecuting the archival pipeline
            return
        elif wflow['RequestStatus'] in ['announced', 'rejected', 'aborted-completed']:
            # Workflows reaching this block are ready for the full pipeline execution
            for pline in self.cleanuplines:
                try:
                    pline.run(wflow)
                except MSRuleCleanerResolveParentError as ex:
                    msg = "%s: Parentage Resolve Error: %s. " % (pline.name, str(ex))
                    msg += "Will retry again in the next cycle."
                    self.logger.error(msg)
                    self._checkStatusAdvanceExpired(wflow, additionalInfo=msg)
                    continue
                except Exception as ex:
                    msg = "%s: General error from pipeline. Workflow: %s. Error:  \n%s. " % (pline.name, wflow['RequestName'], str(ex))
                    msg += "\nWill retry again in the next cycle."
                    self.logger.exception(msg)
                    self._checkStatusAdvanceExpired(wflow, additionalInfo=msg)
                    continue
                if wflow['CleanupStatus'][pline.name]:
                    self.wfCounters['cleaned'][pline.name] += 1
        else:
            # We shouldn't be here:
            msg = "Skipping workflow: %s - "
            msg += "Does not fall under any of the defined categories."
            self.logger.error(msg, wflow['RequestName'])

        # Archive:
        try:
            self.plineArchive.run(wflow)
            if wflow['ForceArchive']:
                self.wfCounters['archived']['forceArchived'] += 1
            else:
                self.wfCounters['archived']['normalArchived'] += 1
        except MSRuleCleanerArchivalSkip as ex:
            msg = "%s: Proper conditions not met: %s. "
            msg += "Skipping archival in the current cycle."
            self.logger.info(msg, wflow['PlineMarkers'][-1], str(ex))
        except MSRuleCleanerArchivalError as ex:
            msg = "%s: Archival Error: %s. "
            msg += "Will retry again in the next cycle."
            self.logger.error(msg, wflow['PlineMarkers'][-1], str(ex))
        except Exception as ex:
            msg = "%s General error from pipeline. Workflow: %s. Error: \n%s. "
            msg += "\nWill retry again in the next cycle."
            self.logger.exception(msg, wflow['PlineMarkers'][-1], wflow['RequestName'], str(ex))

    def setPlineMarker(self, wflow, pName):
        """
        A function intended to mark which is the pipeline currently working
        on the workflow. It is supposed to be called always as a first function
        in the pipeline.
        :param  wflow:   A MSRuleCleaner workflow representation
        :param  pName:   The name of the functional pipeline
        :return:         The workflow object
        """
        # NOTE: The current functional pipeline MUST always be appended at the
        #       end of the 'PlineMarkers' list

        # First get rid of the default:
        if not wflow['PlineMarkers']:
            wflow['PlineMarkers'] = []

        # Then push our current value into the markers list:
        wflow['PlineMarkers'].append(pName)

        # Populate the list of flags to be used later:
        if pName not in wflow['RulesToClean']:
            if pName in self.cleanupPipeNames:
                wflow['RulesToClean'][pName] = []
        if pName not in wflow['CleanupStatus']:
            if pName in self.cleanupPipeNames:
                wflow['CleanupStatus'][pName] = False
        return wflow

    def _getLastStatusTransitionTime(self, wflow):
        """
        A method to return status transition time for a workflow.
        :param wflow:  A MSRuleCleanerWorkflow instance
        :return:       The UTC timestamp for the transition if it exists, None otherwise
        """
        try:
            for trans in wflow['RequestTransition'][::-1]:
                if trans['Status'] == wflow['RequestStatus']:
                    return trans['UpdateTime']
        except KeyError:
            self.logger.error("Missing or broken status transition history for %s", wflow['RequestName'])
            return None

    def _checkStatusAdvanceExpired(self, wflow, additionalInfo=""):
        """
        A method to check if a given status transition has timed out e.g. before
        an alarm being set. The timeout should be configurable and hence taken
        from the instance attributes. Preserve any additional info inside the wflow object.
        :param wflow:          A MSRuleCleanerWorkflow instance
        :param additionalInfo: A string with additional info to be preserved within the workflow for later use by the alarm itself. (Default: "")
        :return:               Bool: True if status alarm time has expired False otherwise
        """
        statusAdvanceExpired = False
        currentTime = int(time.time())
        alarmThreshold = self.msConfig['archiveAlarmHours'] * 3600
        transitionTime = self._getLastStatusTransitionTime(wflow)

        if transitionTime and (currentTime - transitionTime) > alarmThreshold:
            statusAdvanceExpired = True
            if wflow['StatusAdvanceExpiredMsg']:
                wflow['StatusAdvanceExpiredMsg'] += "\n"
            wflow['StatusAdvanceExpiredMsg'] += additionalInfo
        return statusAdvanceExpired

    def _checkClean(self, wflow):
        """
        An auxiliary function used to only check the temporary cleanup status.
        It basically takes the pipelines registered in 'PlineMarkers' that have
        already worked on the workflow as a mask and applies this mask over
        the set of flags in the 'CleanupStatus' field and then reduces the
        result to a single bool value
        """
        # NOTE: This is one of the few functions taking a workflow as an argument
        #       but returning a bool, since it is an auxiliary function and is not
        #       supposed to be called as a standalone function in a pipeline.
        # NOTE: `all([]) == True`, ergo all the 'rejected' && 'aborted-completed' workflows
        #       are also counted as properly cleaned and can trigger archival later

        # Build a list of bool flags based on the mask of PlineMarkers
        cleanFlagsList = [wflow['CleanupStatus'][key]
                          for key in wflow['PlineMarkers']
                          if key in wflow['CleanupStatus']]

        # If no one have worked on the workflow set the clean status to false
        if not wflow['PlineMarkers']:
            cleanStatus = False
        # If we have a mask longer than the list of flags avoid false positives
        # because of the behavior explained above - `all([]) == True`
        elif not cleanFlagsList:
            cleanStatus = False
        # Figure out the final value
        else:
            cleanStatus = all(cleanFlagsList)
        return cleanStatus

    def setClean(self, wflow):
        """
        A function to set the 'IsClean' flag based on the status from all the
        pipelines which have worked on the workflow (and have put their markers
        in the 'PlineMarkers' list)
        :param  wflow:      A MSRuleCleaner workflow representation
        :return:            The workflow object
        """
        wflow['IsClean'] = self._checkClean(wflow)
        return wflow

    def _checkLogDBClean(self, wflow):
        """
        An auxiliary function used to only check the LogDB cleanup status.
        It makes a query to LogDB in order to verify there are no any records for
        the current workflow
        :param wflow:       A MSRuleCleaner workflow representation
        :return:            True if no records were found in LogDB about wflow
        """
        cleanStatus = False
        logDBRecords = self.logDB.get(wflow['RequestName'])
        self.logger.debug("logDBRecords: %s", pformat(logDBRecords))
        if not logDBRecords:
            cleanStatus = True
        return cleanStatus

    def setLogDBClean(self, wflow):
        """
        A function to set the 'IsLogDBClean' flag based on the presence of any
        records in LogDB for the current workflow.
        :param  wflow:      A MSRuleCleaner workflow representation
        :return:            The workflow object
        """
        wflow['IsLogDBClean'] = self._checkLogDBClean(wflow)
        if not wflow['IsLogDBClean'] and wflow['IsArchivalDelayExpired']:
            wflow['IsLogDBClean'] = self._cleanLogDB(wflow)
        return wflow

    def _cleanLogDB(self, wflow):
        """
        A function to be used for cleaning all the records related to a workflow in logDB.
        :param wflow:       A MSRuleCleaner workflow representation
        :return:            True if NO errors were encountered while deleting
                            records from LogDB
        """
        cleanStatus = False
        try:
            if self.msConfig['enableRealMode']:
                self.logger.info("Deleting %s records from LogDB WMStats...", wflow['RequestName'])
                res = self.logDB.delete(wflow['RequestName'], agent=False)
                if res == 'delete-error':
                    msg = "Failed to delete logDB docs for wflow: %s" % wflow['RequestName']
                    raise MSRuleCleanerArchivalError(msg)
                cleanStatus = True
            else:
                self.logger.info("DRY-RUN: NOT Deleting %s records from LogDB WMStats...", wflow['RequestName'])
        except Exception as ex:
            msg = "General Exception while cleaning LogDB records for wflow: %s : %s"
            self.logger.exception(msg, wflow['RequestName'], str(ex))
        return cleanStatus

    def findTargetStatus(self, wflow):
        """
        Find the proper targeted archival status
        :param  wflow:      A MSRuleCleaner workflow representation
        :return:            The workflow object
        """
        # Check the available status transitions before we decide the final status
        targetStatusList = RequestStatus.REQUEST_STATE_TRANSITION.get(wflow['RequestStatus'], [])
        for status in targetStatusList:
            if self.targetStatusRegex.match(status):
                wflow['TargetStatus'] = status
        self.logger.debug("TargetStatus: %s", wflow['TargetStatus'])
        return wflow

    def _checkArchDelayExpired(self, wflow):
        """
        A function to check Archival Expiration Delay based on the information
        returned by WMStatsServer regarding the time of the last request status transition
        :param wflow:      MSRuleCleaner workflow representation
        :return:           True if the archival delay have been expired
        """
        archDelayExpired = False
        currentTime = int(time.time())
        threshold = self.msConfig['archiveDelayHours'] * 3600
        lastTransitionTime = self._getLastStatusTransitionTime(wflow)
        if lastTransitionTime and (currentTime - lastTransitionTime) > threshold:
            archDelayExpired = True
        return archDelayExpired

    def setArchivalDelayExpired(self, wflow):
        """
        A function to set the 'IsArchivalDelayExpired' flag
        """
        wflow['IsArchivalDelayExpired'] = self._checkArchDelayExpired(wflow)
        return wflow

    def archive(self, wflow):
        """
        Move the workflow to the proper archived status after checking
        the full cleanup status
        :param  wflow:      A MSRuleCleaner workflow representation
        :return:            The workflow object
        """
        # Make all the needed checks before trying to archive
        if not (wflow['ParentageResolved'] or wflow['ForceArchive']):
            msg = f"Not properly cleaned workflow: {wflow['RequestName']} - 'ParentageResolved' flag set to false."
            if self._checkStatusAdvanceExpired(wflow, additionalInfo=msg):
                self.alertStatusAdvanceExpired(wflow)
        if not (wflow['IsClean'] or wflow['ForceArchive']):
            msg = "Not properly cleaned workflow: %s" % wflow['RequestName']
            if self._checkStatusAdvanceExpired(wflow, additionalInfo=msg):
                self.alertStatusAdvanceExpired(wflow)
            raise MSRuleCleanerArchivalSkip(msg)
        if not wflow['TargetStatus']:
            msg = "Could not determine which archival status to target for workflow: %s" % wflow['RequestName']
            if self._checkStatusAdvanceExpired(wflow, additionalInfo=msg):
                self.alertStatusAdvanceExpired(wflow)
            raise MSRuleCleanerArchivalError(msg)
        if not wflow['IsLogDBClean']:
            msg = "LogDB records have not been cleaned for workflow: %s" % wflow['RequestName']
            if self._checkStatusAdvanceExpired(wflow, additionalInfo=msg):
                self.alertStatusAdvanceExpired(wflow)
            raise MSRuleCleanerArchivalSkip(msg)
        if not wflow['IsArchivalDelayExpired']:
            msg = "Archival delay period has not yet expired for workflow: %s." % wflow['RequestName']
            raise MSRuleCleanerArchivalSkip(msg)
        if not self.msConfig['enableRealMode']:
            msg = "Real Run Mode not enabled."
            raise MSRuleCleanerArchivalSkip(msg)

        # Proceed with the actual archival:
        try:
            self.reqmgr2.updateRequestStatus(wflow['RequestName'], wflow['TargetStatus'])
            msg = "Successful status transition to: %s for workflow: %s"
            self.logger.info(msg, wflow['TargetStatus'], wflow['RequestName'])
        except Exception as ex:
            msg = "General Exception while trying status transition to: %s " % wflow['TargetStatus']
            msg += "for workflow: %s : %s" % (wflow['RequestName'], str(ex))
            raise MSRuleCleanerArchivalError(msg) from None
        return wflow

    def getMSOutputTransferInfo(self, wflow):
        """
        Fetches the transfer information from the MSOutput REST interface for
        the given workflow.
        :param  wflow:   A MSRuleCleaner workflow representation
        :return:         The workflow object
        """
        headers = {'Accept': 'application/json'}
        params = {}
        url = '%s/data/info?request=%s' % (self.msConfig['msOutputUrl'],
                                           wflow['RequestName'])
        try:
            res = self.curlMgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
            data = json.loads(res)['result'][0]
            transferInfo = data['transferDoc']
        except Exception as ex:
            transferInfo = None
            msg = "General exception while fetching TransferInfo from MSOutput for %s. "
            msg += "Error: %s"
            self.logger.exception(msg, wflow['RequestName'], str(ex))

        if transferInfo is None:
            msg = f"Workflow {wflow['RequestName']} is still missing the output transfer document."
            self.logger.warning(msg)
        elif transferInfo['TransferStatus'] == 'pending':
            msg = f"Workflow {wflow['RequestName']} is still pending the output data placement."
            self.logger.warning(msg)
        elif transferInfo['TransferStatus'] == 'done':
            # Set Transfer status - information fetched from MSOutput only
            wflow['TransferDone'] = True

            # Set Tape rules status - information fetched from Rucio (tape rule ids from MSOutput)
            # For setting 'TransferTape' = True we require either no tape rules for the
            # workflow have been created or all existing tape rules to be in status 'OK',
            # so every empty TapeRuleID we consider as completed.
            tapeRulesStatusList = []
            for mapRecord in transferInfo['OutputMap']:
                if not mapRecord['TapeRuleID']:
                    continue

                rucioRule = self.rucio.getRule(mapRecord['TapeRuleID'])
                if not rucioRule:
                    msg = "Tape rule: %s not found for workflow: %s "
                    msg += "Possible server side error."
                    self.logger.error(msg, mapRecord['TapeRuleID'], wflow['RequestName'])
                    rucioRule = {'state': 'Missing'}

                if rucioRule['state'] == 'OK':
                    tapeRulesStatusList.append(True)
                    msg = "Tape rule: %s in final state: %s for workflow: %s"
                    self.logger.info(msg, mapRecord['TapeRuleID'], rucioRule['state'], wflow['RequestName'])
                else:
                    tapeRulesStatusList.append(False)
                    msg = "Tape rule: %s in non final state: %s for workflow: %s"
                    self.logger.info(msg, mapRecord['TapeRuleID'], rucioRule['state'], wflow['RequestName'])

                wflow['TapeRulesStatus'].append((mapRecord['TapeRuleID'], rucioRule['state'], mapRecord['Dataset']))

            if all(tapeRulesStatusList):
                wflow['TransferTape'] = True

        return wflow

    def setParentDatasets(self, wflow):
        """
        Used to resolve parent datasets for a workflow.
        :param  wflow:   A MSRuleCleaner workflow representation
        :return:         The workflow object
        """
        if wflow['InputDataset'] and wflow['IncludeParents']:
            childDataset = wflow['InputDataset']
            parentDataset = findParent([childDataset], self.msConfig['dbsUrl'])
            # NOTE: If findParent() returned None then the DBS service failed to
            #       resolve the request (it is considered an ERROR outside WMCore)
            if parentDataset.get(childDataset, None) is None:
                msg = "Failed to resolve parent dataset for: %s in workflow: %s" % (childDataset, wflow['RequestName'])
                raise MSRuleCleanerResolveParentError(msg)
            elif parentDataset:
                wflow['ParentDataset'] = [parentDataset[childDataset]]
                msg = "Found parent %s for input dataset %s in workflow: %s "
                self.logger.info(msg, parentDataset, wflow['InputDataset'], wflow['RequestName'])
            else:
                msg = "Could not find parent for input dataset: %s in workflows: %s"
                self.logger.error(msg, wflow['InputDataset'], wflow['RequestName'])
        return wflow

    def getRucioRules(self, wflow, gran, rucioAcct):
        """
        Queries Rucio and builds the relevant list of blocklevel rules for
        the given workflow
        :param  wflow:   A MSRuleCleaner workflow representation
        :param  gran:    Data granularity to search for Rucio rules. Possible values:
                         'block' or 'container'
        :return:         The workflow object
        """
        currPline = wflow['PlineMarkers'][-1]

        # Create the container list to the rucio account map and set the checkGlobalLocks flag.
        mapRuleType = {self.msConfig['rucioWmaAccount']: ["OutputDatasets"],
                       self.msConfig['rucioMStrAccount']: ["InputDataset", "ParentDataset"]}
        if rucioAcct == self.msConfig['rucioMStrAccount']:
            checkGlobalLocks = True
        else:
            checkGlobalLocks = False

        # Find all the data placement rules created by the components:
        for dataType in mapRuleType[rucioAcct]:
            dataList = wflow[dataType] if isinstance(wflow[dataType], list) else [wflow[dataType]]
            for dataCont in dataList:
                if dataCont is None:
                    continue
                self.logger.debug("getRucioRules: dataCont: %s", pformat(dataCont))
                if checkGlobalLocks and dataCont in self.globalLocks:
                    msg = "Found dataset: %s in GlobalLocks. NOT considering it for filling the "
                    msg += "RulesToClean list for both container and block level Rules for workflow: %s!"
                    self.logger.info(msg, dataCont, wflow['RequestName'])
                    continue
                if gran == 'container':
                    ruleIds = [rule['id'] for rule in self.rucio.listDataRules(dataCont, account=rucioAcct)]
                    if ruleIds:
                        wflow['RulesToClean'][currPline].extend(ruleIds)
                        msg = "Container %s has the following container-level rules to be removed: %s"
                        self.logger.info(msg, dataCont, ruleIds)
                elif gran == 'block':
                    try:
                        blocks = self.rucio.getBlocksInContainer(dataCont)
                        for block in blocks:
                            for rule in self.rucio.listDataRules(block, account=rucioAcct):
                                wflow['RulesToClean'][currPline].append(rule['id'])
                                msg = "Found %s block-level rule to be deleted for container %s"
                                self.logger.info(msg, rule['id'], dataCont)
                    except WMRucioDIDNotFoundException:
                        msg = "Container: %s not found in Rucio for workflow: %s."
                        self.logger.info(msg, dataCont, wflow['RequestName'])
        return wflow

    def cleanRucioRules(self, wflow):
        """
        Cleans all the Rules present in the field 'RulesToClean' in the MSRuleCleaner
        workflow representation. And fills the relevant Cleanup Status.
        :param wflow:   A MSRuleCleaner workflow representation
        :return:        The workflow object
        """
        # NOTE: The function should be called independently and sequentially from
        #       The Input and the respective BlockLevel pipelines.

        # NOTE: The current functional pipeline is always the last one in the PlineMarkers list
        currPline = wflow['PlineMarkers'][-1]
        delResults = []
        if self.msConfig['enableRealMode']:
            for rule in wflow['RulesToClean'][currPline]:
                self.logger.info("%s: Deleting ruleId: %s ", currPline, rule)
                delResult = self.rucio.deleteRule(rule)
                delResults.append(delResult)
                if not delResult:
                    self.logger.warning("%s: Failed to delete ruleId: %s ", currPline, rule)
        else:
            for rule in wflow['RulesToClean'][currPline]:
                delResults.append(True)
                self.logger.info("%s: DRY-RUN: Is about to delete ruleId: %s ", currPline, rule)

        # Set the cleanup flag:
        wflow['CleanupStatus'][currPline] = all(delResults)
        return wflow

    def getRequestRecords(self, reqStatus):
        """
        Queries ReqMgr2 for requests in a given status.
        :param reqStatus: The status for the requests to be fetched from ReqMgr2
        :return requests: A dictionary with all the workflows in the given status
        """
        self.logger.info("Fetching requests in status: %s", reqStatus)
        result = self.reqmgr2.getRequestByStatus([reqStatus], detail=True)
        if not result:
            requests = {}
        else:
            requests = result[0]
        self.logger.info('  retrieved %s requests in status: %s', len(requests), reqStatus)
        return requests

    def alertStatusAdvanceExpired(self, wflow):
        """
        Send alert notifying that a workflow has spend too much time in a given status
        :param wflow: MSRuleCleanerWorkflow instance
        :additionalInfo: String with additional information.
        :return: none
        """
        lastTransitionTime = self._getLastStatusTransitionTime(wflow)
        alertName = "{}: Not archived workflow: {}".format(self.alertServiceName, wflow['RequestName'])
        alertSeverity = "medium"
        alertDescription = "Found a workflow not archived since {}.".format(time.ctime(lastTransitionTime))
        alertDescription += "\nWorkflow: \n{}".format(pformat({wfKey: wflow[wfKey] for wfKey in ['RequestName',
                                                                                                 'ParentageResolved',
                                                                                                 'TransferDone',
                                                                                                 'TransferTape',
                                                                                                 'TapeRulesStatus']}))
        alertDescription += "\nHas exceeded the Status Advance Timeout of: {} hours".format(self.msConfig['archiveAlarmHours'])
        alertDescription += " for its current status: {}.".format(wflow['RequestStatus'])
        if wflow['StatusAdvanceExpiredMsg']:
            alertDescription += "\nAdditional info: \n{}".format(wflow['StatusAdvanceExpiredMsg'])
        alertDescription += "\nActions need to be taken!"
        alertSummary = "[MSRuleCleaner] Found workflow: {} not archived since {}.".format(wflow['RequestName'], time.ctime(lastTransitionTime))

        # Check if alarms are enabled for this service
        # Alert expiration time defaults to 2 days
        if self.msConfig["sendNotification"]:
            tag = self.alertDestinationMap.get("alertStatusAdvanceExpired", "")
            self.sendAlert(alertName, alertSeverity, alertSummary, alertDescription,
                           service=self.alertServiceName, endSecs=self.alertExpiration, tag=tag)
        self.logger.critical(alertDescription)
