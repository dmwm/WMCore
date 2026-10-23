from __future__ import (division, print_function)

import json
import time
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException

from WMCore.Services.pycurl_manager import RequestHandler
from Utils.CertTools import getKeyCertFromEnv


def getdata(url, params, headers=None):
    "Helper function to get data from the service"
    ckey, cert = getKeyCertFromEnv()
    mgr = RequestHandler()
    res = mgr.getdata(url, params=params, headers=headers, ckey=ckey, cert=cert)
    return json.loads(res)

def format_timestamp(timestamp_float):
    """Converts a float timestamp (seconds since epoch) to a readable string."""
    # This format gives you: "2025-12-09 19:22:15"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp_float))


class InputDataRucioRuleCleaner(CherryPyPeriodicTask):
    """
    A periodic CherryPy task that cleans block-level Rucio replication rules for
    input datasets of completed GlobalWorkQueue elements.

    Overview
    --------
    The GlobalWorkQueue holds elements representing units of work, each associated
    with one or more input data blocks. When a workflow finishes processing a block,
    its Rucio replication rule (which was created to stage the data to a site) is
    no longer needed and should be removed to free storage quota.

    This task runs periodically and performs the following steps each cycle:

    1. Fetch Done elements
       Query CouchDB for GlobalWorkQueue elements with Status='Done' only, avoiding
       a full table scan of all elements.

    2. Skip already-processed elements
       An in-memory set (_processedElementIds) tracks elements whose rules were fully
       cleaned in a previous cycle. These are skipped immediately with an O(1) lookup,
       avoiding redundant HTTP and CouchDB calls. The set is trimmed each cycle to
       only IDs still present in the queue, preventing unbounded memory growth.

    3. Filter by completion
       Only elements with PercentComplete == 100 and PercentSuccess == 100 are
       considered. Status='Done' alone does not guarantee full success (some jobs
       may have failed), so this additional check ensures we only clean rules for
       fully successful elements.

    4. Check whether each block's rule can be deleted (canDeleteRucioRule)
       A block's Rucio rule must not be deleted if another active workflow is still
       using the same input data. For each block:
         a. Query ReqMgr2 for all requests that use the same input container
            (dataCont = block without the #hash suffix). Results are cached by
            dataCont in a per-cycle dict (reqmgr2Cache) so that multiple blocks
            sharing the same container only trigger one HTTP call per cycle.
         b. For each other active request found, query the GlobalWorkQueue for its
            elements and check whether the specific block has been fully processed
            (PercentComplete == 100 and PercentSuccess == 100). If any other request
            is still processing the block, deletion is deferred to the next cycle.

    5. Collect and delete Rucio rules
       For blocks cleared in step 4, query Rucio for existing replication rules
       (listDataRules). Rules already deleted in a previous cycle return an empty
       list and are silently skipped. Remaining rules are batched into a single
       cleanRucioRules call per element, which sets their Rucio lifetime to 0
       (effectively scheduling them for deletion).

    6. Track element completion
       An element is added to _processedElementIds only when ALL of the following
       hold for that cycle:
         - No block was deferred (canDeleteRucioRule returned True for every block)
         - No unexpected error occurred during rule lookup
         - All Rucio rule updates reported success (CleanupStatus == True)
       If any condition fails, the element is re-evaluated next cycle.

    Scalability notes
    -----------------
    - getElements(status='Done') uses the elementsByStatus CouchDB index, avoiding
      a full queue scan.
    - _processedElementIds eliminates repeat processing of already-cleaned elements
      across cycles, so the active working set per cycle is bounded to newly Done
      elements since the last cycle.
    - reqmgr2Cache reduces ReqMgr2 HTTP calls from O(elements x blocks) to O(unique
      containers) per cycle.
    """

    def __init__(self, rest, config):

        super(InputDataRucioRuleCleaner, self).__init__(config)
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)
        self.msRuleCleaner = MSRuleCleaner(config.msRuleCleaner, logger=self.logger)
        self._processedElementIds = set()  # element IDs confirmed with all block rules fully cleaned in this process lifetime

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.cleanRucioRules, 'duration': config.cleanInputDataRucioRuleDuration}]

    def getRequestForInputDataset(self, inputdataset, reqmgr2Url):
        url = f"{reqmgr2Url}/data/request?inputdataset={inputdataset}"
        params = {}
        headers = {"Accept": "application/json"}
        res = None
        try:
            res = getdata(url, params, headers)
        except Exception as ex:
            msg = "General exception while fetching requests from ReqMgr2 for inputdataset %s"
            self.logger.exception(msg, inputdataset, str(ex))
        return res

    def canDeleteRucioRule(self, currentRequestName, block, dataCont, config, reqmgr2Cache=None):
        """
        Check if the Rucio rule for the given block can be deleted.
        :param currentRequestName: The name of the current request being processed
        :param block: The data block to check
        :param dataCont: The container name extracted from the block
        :param config: The configuration object
        :param reqmgr2Cache: Optional dict caching ReqMgr2 responses by dataCont to avoid redundant HTTP calls
        :return: True if the rule can be deleted, False otherwise
        """
        try:
            # Use the cache to avoid repeated HTTP calls for the same container within a cycle.
            if reqmgr2Cache is not None and dataCont in reqmgr2Cache:
                response = reqmgr2Cache[dataCont]
            else:
                response = self.getRequestForInputDataset(dataCont, config.msRuleCleaner['reqmgr2Url'])
                if reqmgr2Cache is not None:
                    reqmgr2Cache[dataCont] = response

            if not response or "result" not in response:
                self.logger.warning(f"Failed to fetch requests using dataset {dataCont}. Response: {response}")
                return False  # We do not know what is going on, better not delete the rule

            requestsUsingData = response["result"][0]

            for request_id, request_data in requestsUsingData.items():
                self.logger.debug(f"Check request: {request_data['RequestName']}")

                if request_data['RequestName'] == currentRequestName:
                    self.logger.debug(f"Request {request_data['RequestName']} is the current request. Continuing to next request.")
                    continue

                # Only consider workflows in active statuses — skip requests that are already done or cancelled
                if request_data['RequestStatus'] not in ['new', 'assignment-approved', 'assigned', 'staging', 'acquired', 'staged', 'running-open', 'running-closed']:
                    self.logger.debug(f"Request {request_data['RequestName']} is in status {request_data['RequestStatus']}. Continuing to next request.")
                    continue

                try:
                    otherRequestElements = self.globalQ.backend.getElements(WorkflowName=request_data['RequestName'])

                    if not otherRequestElements:
                        self.logger.info(f"No workqueue elements found for request {request_id}: {request_data}. The workqueue might not have been created yet.")
                        return False  # We do not know what is going on, better not delete the rule

                    for otherElement in otherRequestElements:

                        if block not in otherElement.get('Inputs'):
                            continue

                        percentComplete = otherElement.get('PercentComplete', 0)
                        percentSuccess = otherElement.get('PercentSuccess', 0)

                        if percentComplete < 100 or percentSuccess < 100:
                            self.logger.debug(f"Rule for block {block} cannot be deleted. Workqueue elements of request {request_data['RequestName']} using the same block have not completed processing ({percentComplete}, {percentSuccess}).")
                            return False

                except Exception as ex:
                    self.logger.error(f"Error while finding elements for request {request_id}: {request_data} and making consideration on data processing completion: {str(ex)}")
                    return False  # We do not know what is going on, better not delete the rule

            return True

        except Exception as ex:
            self.logger.error(f"Error while checking if rule for block {block} can be deleted: {str(ex)}")
            return False  # We do not know what is going on, better not delete the rule

    def cleanRucioRules(self, config):
        """
        Queries global queue and builds the list of blocklevel Rucio rules of finished elements to be deleted.
        Calls MSRuleCleaner cleanRucioRules(self, wflow) to delete the rules.
        :config:  The configuration for the task. This uses Rucio account from config to use for querying rules
        :return:  True if any cleaning happened this cycle, False otherwise.
        """

        tStart = time.time()

        #globalQueueElements = self.globalQ.backend.getElements(status='Done')
        globalQueueElements = self.globalQ.backend.getElements()

        # Trim skip-set to only IDs still present in the queue, preventing unbounded growth
        currentIds = {el.id for el in globalQueueElements}
        self._processedElementIds &= currentIds

        # Per-cycle cache: dataCont -> ReqMgr2 response, shared across all elements and blocks.
        # Avoids redundant HTTP calls for blocks sharing the same container within a cycle.
        reqmgr2Cache = {}

        do_cleaning = False

        if globalQueueElements:
            current_time = format_timestamp(time.time())
            self.logger.info(f"{current_time}: Found {len(globalQueueElements)} globalqueue elements ({len(self._processedElementIds)} already fully processed, skipping).")

            for element in globalQueueElements:

                if element.id in self._processedElementIds:
                    continue

                requestName = element.get('RequestName')
                percentComplete = element.get('PercentComplete', 0)
                percentSuccess = element.get('PercentSuccess', 0)

                if percentComplete == 100 and percentSuccess == 100:
                    self.logger.info("Element %s workflow=%s status=%s PercentComplete=%s PercentSuccess=%s",
                                     element.id, requestName, element.get('Status'), percentComplete, percentSuccess)

                    # Structure required by MSRuleCleaner.cleanRucioRules()
                    rulesToClean = {'PlineMarkers': ['Current'], 'RulesToClean': {'Current': []}, 'CleanupStatus': {'Current': []}}

                    blocks = element.get('Inputs')

                    cleanedRules_info = {}
                    elementFullyProcessed = True  # flipped to False on any deferral, error, or partial Rucio failure
                    if blocks:
                        for block in blocks:
                            dataCont = block.split('#')[0]  # strip block hash to get the container name

                            if not self.canDeleteRucioRule(requestName, block, dataCont, config, reqmgr2Cache):
                                elementFullyProcessed = False
                                continue

                            try:
                                rules = self.msRuleCleaner.rucio.listDataRules(block, account=config.msRuleCleaner['rucioAccount'])
                                if rules:
                                    # one block can have multiple rules
                                    cleanedRules_info[block] = {'id': [], 'bytes': []}
                                    for rule in rules:
                                        cleanedRules_info[block]['id'].append(rule['id'])
                                        cleanedRules_info[block]['bytes'].append(rule['bytes'])
                                        # cleanRucioRules expects rule ids under the last PlineMarker
                                        rulesToClean['RulesToClean'][rulesToClean['PlineMarkers'][-1]].append(rule['id'])
                                else:
                                    msg = "Rucio rule for block: %s not found for workflow: %s."
                                    self.logger.debug(msg, block, requestName)
                            except WMRucioDIDNotFoundException:
                                msg = "Exception when cleaning Rucio rule for block: %s of workflow: %s."
                                self.logger.debug(msg, block, requestName)
                                continue
                            except Exception as ex:
                                self.logger.error(f"Unexpected error fetching rules for block {block} of workflow {requestName}: {str(ex)}")
                                elementFullyProcessed = False
                                continue

                    if cleanedRules_info:
                        current_time = format_timestamp(time.time())
                        self.logger.info(f"{current_time}: Start cleaning rules for completed element {element.id}")

                        do_cleaning = True

                        for block, info in cleanedRules_info.items():
                            for rule_id, size in zip(info["id"], info["bytes"]):
                                self.logger.info(f"{current_time} Rule to clean: {rule_id} {block} {size} {requestName}")

                        self.msRuleCleaner.cleanRucioRules(rulesToClean)
                        if not rulesToClean['CleanupStatus']['Current']:
                            elementFullyProcessed = False

                        current_time = format_timestamp(time.time())
                        self.logger.info(f"{current_time}: End cleaning rules for completed element {element.id} (success={elementFullyProcessed})")

                    if elementFullyProcessed:
                        self._processedElementIds.add(element.id)

            if not do_cleaning:
                current_time = format_timestamp(time.time())
                self.logger.debug(f"{current_time} No cleaning happened: There are no completed workqueue elements or block is currently used by other requests or rules already cleaned")

        else:
            current_time = format_timestamp(time.time())
            self.logger.debug(f"{current_time} No elements found in GlobalQueue")

        current_time = format_timestamp(time.time())
        self.logger.info(f"{current_time} {self.__class__.__name__} executed in {(time.time() - tStart):.3f} secs.")
        return do_cleaning
