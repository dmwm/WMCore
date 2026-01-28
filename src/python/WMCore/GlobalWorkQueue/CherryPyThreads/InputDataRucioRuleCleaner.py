from __future__ import (division, print_function)

import time
#import json
from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.MicroService.MSRuleCleaner.MSRuleCleaner import MSRuleCleaner

from WMCore.Services.Rucio.Rucio import WMRucioDIDNotFoundException

from WMCore.ReqMgr.Web.ReqMgrService import getdata
#from WMCore.Services.pycurl_manager import RequestHandler
#from Utils.CertTools import ckey, cert

def format_timestamp(timestamp_float):
    """Converts a float timestamp (seconds since epoch) to a readable string."""
    # This format gives you: "2025-12-09 19:22:15"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp_float))

'''
def canDeleteRucioRule(self, currentRequestName, block, dataCont, config):
    """
    Check if the Rucio rule for the given block can be deleted.
    :param currentRequest: The name of the current request being processed
    :param block: The data block to check
    :param dataCont: The container name extracted from the block
    :param config: The configuration object
    :return: True if the rule can be deleted, False otherwise
    """
    try:
        # Step 1: Find the requests that use the same input data
        #url = 'https://cmsweb.cern.ch/reqmgr2/data/request?outputdataset=%s' % dataset
        #params = {}
        #headers = {'Accept': 'application/json'}
        #https://gitlab.cern.ch/cmsweb-k8s/services_config/-/blob/test/config.test16/workqueue/config.py?ref_type=heads
        #config.msRuleCleaner['reqmgr2Url'] = "%s/reqmgr2" % BASE_URL
        #url = config.msRuleCleaner['reqmgr2Url']+'/data/request?inputdataset=%s' % dataCont
        url = f"{config.msRuleCleaner['reqmgr2Url']}/data/request?inputdataset={dataCont}"
        params = {}
        headers = {"Accept": "application/json"}
        
        # Use the getdata function to fetch the requests using the current input data
        response = getdata(url, params, headers)
        
        if not response or "result" not in response:
            self.logger.warning(f"Failed to fetch requests using dataset {dataCont}. Response: {response}")
            return True  # Assume no requests if the response is invalid
        
        requestsUsingData = response["result"]
        
        # Step 1a: If there are no requests, return True. No need since this always returns at least the current request
        #if not requestsUsingData:
        #    self.logger.info(f"No requests are using dataset {dataCont}. Rule for block {block} can be deleted.")
        #    return True
        
        #self.logger.info(f"Dataset {dataCont} is in use by the following requests: {requestsUsingData}")
        
        # Step 2: Find the workqueue elements of those requests
        foundElements = False  # Track if any workqueue elements are found
        for request in requestsUsingData:
            # Skip the current request
            if request['RequestName'] == currentRequestName:
                continue
            
            # if the workflow is done etc return True. We can delete the rule

            try:
                # Query the global queue for elements of the other request
                otherRequestElements = self.globalQ.backend.getElements(WorkflowName=request['RequestName'])
                
                # Step 3: If there are no workqueue elements for this request, continue to the next request
                if not otherRequestElements:
                    self.logger.info(f"No workqueue elements found for request {request}. The workqueue might not have been created yet.")
                    continue
                
                foundElements = True  # At least one element is found at other request
                
                # Step 4: Check the status of these workqueue element that uses the same datablock
                for otherElement in otherRequestElements:
                    
                    if block not in otherElement.get('Inputs'): continue
                    
                    percentComplete = otherElement.get('PercentComplete', 0)  # Default to 0 if key is missing
                    percentSuccess = otherElement.get('PercentSuccess', 0)  # Default to 0 if key is missing
                    
                    # Step 5: If any workqueue element is not completed, return False
                    if percentComplete < 100 or percentSuccess < 100:
                        self.logger.info(f"Workqueue element {otherElement.get('id')} for request {request['RequestName']} is not yet completed. Rule for block {block} cannot be deleted.")
                        return False

            except Exception as ex:
                self.logger.error(f"Error while finding elements for request {request}: {str(ex)}")
                return False
        
        # Step 3 (fixed): If no elements were found for all requests, return False
        if not foundElements:
            self.logger.info(f"No workqueue elements found for any of the requests using dataset {dataCont}. Rule for block {block} cannot be deleted.")
            return False
        
        # Step 4: If all workqueue elements are processed, return True
        self.logger.info(f"All workqueue elements for requests using dataset {dataCont} are completed. Rule for block {block} can be deleted.")
        return True
    
    except Exception as ex:
        self.logger.error(f"Error while checking if rule for block {block} can be deleted: {str(ex)}")
        return False
'''

class InputDataRucioRuleCleaner(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(InputDataRucioRuleCleaner, self).__init__(config)
        self.globalQ = globalQueue(logger=self.logger, **config.queueParams)
        self.msRuleCleaner = MSRuleCleaner(config.msRuleCleaner, logger=self.logger)  # Initialize MSRuleCleaner
        #self.curlMgr = RequestHandler()

    def setConcurrentTasks(self, config):
        """
        sets the list of function reference for concurrent tasks
        """
        self.concurrentTasks = [{'func': self.cleanRucioRules, 'duration': config.cleanInputDataRucioRuleDuration}]

    def getRequestForInputDataset(self, inputdataset, reqmgr2Url):
        # Step 1: Find the requests that use the same input data
        #url = f"{config.msRuleCleaner['reqmgr2Url']}/data/request?inputdataset={inputdataset}"
        url = f"{reqmgr2Url}/data/request?inputdataset={inputdataset}"
        params = {}
        headers = {"Accept": "application/json"}
        res = None
        try:
            #res = self.curlMgr.getdata(url, params=params, headers=headers, ckey=ckey(), cert=cert())
            #res = json.loads(res)
            res = getdata(url, params, headers)
        except Exception as ex:
            msg = "General exception while fetching requests from ReqMgr2 for inputdataset %s"
            self.logger.exception(msg, inputdataset, str(ex))

        # Use the getdata function to fetch the requests using the current input data
        return res

    def canDeleteRucioRule(self, currentRequestName, block, dataCont, config):
        """
        Check if the Rucio rule for the given block can be deleted.
        :param currentRequest: The name of the current request being processed
        :param block: The data block to check
        :param dataCont: The container name extracted from the block
        :param config: The configuration object
        :return: True if the rule can be deleted, False otherwise
        """
        try:
            # Step 1: Find the requests that use the same input data
            #url = f"{config.msRuleCleaner['reqmgr2Url']}/data/request?inputdataset={dataCont}"
            #params = {}
            #headers = {"Accept": "application/json"}
            
            # Use the getdata function to fetch the requests using the current input data
            #response = getdata(url, params, headers)
            response = self.getRequestForInputDataset(dataCont, config.msRuleCleaner['reqmgr2Url'])
            
            if not response or "result" not in response:
                self.logger.warning(f"Failed to fetch requests using dataset {dataCont}. Response: {response}")
                return False  # We do not know what is going on, better not delete the rule
            
            self.logger.info(f"Response: {response}")

            requestsUsingData = response["result"][0]

            self.logger.info(f"Requests: {requestsUsingData}")
    
            for request_id,request_data in requestsUsingData.items():
                # Skip the current request
                if request_data['RequestName'] == currentRequestName:
                    continue
                
                # only consider workflows in good status and not done yet
                if request_data['RequestStatus'] not in ['new', 'assignment-approved', 'assigned', 'staging', 'acquired', 'staged', 'running-open', 'running-closed']:
                    self.logger.info(f"Request {request_data['RequestName']} is in status {request_data['RequestStatus']}. Continuing to next request.")
                    continue
    
                try:
                    # Step 2: Query the global queue for elements of the other request
                    otherRequestElements = self.globalQ.backend.getElements(WorkflowName=request_data['RequestName'])
                    
                    if not otherRequestElements:
                        self.logger.info(f"No workqueue elements found for request {request_id}: {request_data}. The workqueue might not have been created yet.")
                        return False # We do not know what is going on, better not delete the rule
                    
                    # Step 3: Check the status of these workqueue element that uses the same datablock
                    for otherElement in otherRequestElements:
                        
                        if block not in otherElement.get('Inputs'): continue
                        
                        percentComplete = otherElement.get('PercentComplete', 0)  # Default to 0 if key is missing
                        percentSuccess = otherElement.get('PercentSuccess', 0)  # Default to 0 if key is missing
                        
                        if percentComplete < 100 or percentSuccess < 100:
                            self.logger.info(f"Rule for block {block} cannot be deleted. Workqueue elements of request {request_data['RequestName']} using the same block have not completed processing ({percentComplete}, {percentSuccess}).")
                            return False
                
                except Exception as ex:
                    self.logger.error(f"Error while finding elements for request {request_id}: {request_data} and making consideration on data processing completion: {str(ex)}")
                    return False #We do not know what is going on, better not delete the rule     
            
            return True  
        
        except Exception as ex:
            self.logger.error(f"Error while checking if rule for block {block} can be deleted: {str(ex)}")
            return False #We do not know what is going on, better not delete the rule

    def cleanRucioRules(self, config):
        """
        Queries global queue and builds the list of blocklevel Rucio rules of finished elements to be deleted. Calls MSRuleCleaner cleanRucioRules(self, wflow) to delete the rules.
        :config:       The configuration for the task. This uses Rucio account from config to use for querying rules
        :return:       The result of MSRuleCleaner cleanRucioRules(self, wflow) method, which is True if all rules were deleted successfully, False otherwise.
        """
        
        tStart = time.time()
        
        #statuses = ['Available', 'Done', 'Acquired', 'Failed', 'Canceled']
        #globalQueueElements=self.globalQ.getWork({'Status':'Done'},siteJobCounts={})
        globalQueueElements=self.globalQ.backend.getElements()
            
        #print("Elements in GlobalQueue cleanRucioRules:")
        #print(json.dumps(globalQueueElements,indent=2))
        
        do_cleaning = False

        if globalQueueElements:
            #print(f"Found {len(globalQueueElements)} elements in GlobalQueue")
            current_time = format_timestamp(time.time())
            self.logger.info(f"{current_time}: Found {len(globalQueueElements)} globalqueue elements.")

            for element in globalQueueElements:
                
                requestName = element.get('RequestName')  # Extract the RequestName field
                percentComplete = element.get('PercentComplete', 0)  # Default to 0 if key is missing
                percentSuccess = element.get('PercentSuccess', 0)  # Default to 0 if key is missing

                if percentComplete == 100 and percentSuccess == 100:

                    #to be able to use cleanRules method of MSRuleCleaner
                    rulesToClean = {'PlineMarkers':['Current'], 'RulesToClean': {'Current': []}, 'CleanupStatus': {'Current': []}}

                    #'Inputs': {'/MinimumBias/ComissioningHI-v1/RAW#372d624c-089d-11e1-8347-003048caaace':
                    blocks = element.get('Inputs')  # Example key for dataset
                                                       
                    # Fetch rules for blocks
                    cleanedRules_info = {}
                    if blocks:
                        for block in blocks:
                            #print("Adding block ", block, " to RulesToClean")
                            dataCont = block.split('#')[0]  # Extract the container name from the block
                            
                            # Check if the Rucio rule for this block can be deleted
                            if not self.canDeleteRucioRule(requestName, block, dataCont, config):
                                self.logger.info(f"Skipping deletion of rules for block {block} as it is still in use.")
                                continue
 
                            ## Check if the dataset is in use by other requests
                            #try:
                            #    # Assuming there's an endpoint that provides the requests using the input data
                            #    url = f"{config.reqmgrUrl}/data/request_by_input"
                            #    params = {"input": dataCont}
                            #    headers = {"Accept": "application/json"}
                            #    
                            #    # Use the getdata function to fetch the requests using the current input data
                            #    response = getdata(url, params, headers)
                            #    
                            #    # Process the response to extract request names
                            #    if response and "result" in response:
                            #        requestsUsingData = response["result"]
                            #        if requestsUsingData:
                            #            self.logger.info(f"Dataset {dataCont} is still in use by the following requests: {requestsUsingData}")
                            #            continue  # Skip cleaning for this dataset
                            #        else:
                            #            self.logger.info(f"Dataset {dataCont} is not in use by any other requests.")
                            #    else:
                            #        self.logger.warning(f"Failed to fetch requests using dataset {dataCont}. Response: {response}")
                            #except Exception as ex:
                            #    self.logger.error(f"Error while checking requests using dataset {dataCont}: {str(ex)}")
                            #    continue
                            
                            #need to self.getGlobalLocks() before using self.msRuleCleaner.globalLocks
                            #if dataCont in self.msRuleCleaner.globalLocks:
                            #    msg = "Found dataset: %s in GlobalLocks. NOT considering it for filling the "
                            #    msg += "RulesToClean list for both container and block level Rules for workflow: %s!"
                            #    self.logger.info(msg, dataCont, requestName)
                            #    continue
                            try:
                                #print('Fetching rules for block:', block, "\n", config.rucioAccount, "\n", self.msRuleCleaner.rucio.listDataRules(block, account=config.rucioAccount))
                                rules = self.msRuleCleaner.rucio.listDataRules(block, account=config.msRuleCleaner['rucioAccount'])
                                #found rules for this block. If the rules of this block already cleaned there is no rules found
                                if rules:
                                    cleanedRules_info[block] = {}
                                    #one block can have multiple rules
                                    cleanedRules_info[block]['id'] = []
                                    cleanedRules_info[block]['bytes'] = []
                                    for rule in rules:
                                        #msg = "Found %s block-level rule to be deleted for container %s"
                                        #self.logger.info(msg, rule['id'], dataCont)
                                        #current_time = format_timestamp(time.time())
                                        #self.logger.info(f"{current_time}: Rule {rule['id']} {block} {rule['bytes']} {requestName} to be cleaned")
                                        cleanedRules_info[block]['id'].append(rule['id'])
                                        cleanedRules_info[block]['bytes'].append(rule['bytes'])
                                        #cleanRules of MSRuleCleaner expects a list of rule ids and always clean the last one in the list of PlineMarkers
                                        rulesToClean['RulesToClean'][rulesToClean['PlineMarkers'][-1]].append(rule['id'])
                            except WMRucioDIDNotFoundException:
                                msg = "Block: %s not found in Rucio for workflow: %s."
                                self.logger.info(msg, block, requestName)
                                continue
                    
                    if cleanedRules_info:
                        current_time = format_timestamp(time.time())
                        self.logger.info(f"{current_time}: Start cleaning rules for completed element {element.id}")
                        
                        do_cleaning = True

                        for block, info in cleanedRules_info.items():
                            for rule_id, size in zip(info["id"], info["bytes"]):
                                self.logger.info(f"{current_time} Rule to clean: {rule_id} {block} {size} {requestName}")
                        
                        self.msRuleCleaner.cleanRucioRules(rulesToClean)
                        
                        current_time = format_timestamp(time.time())
                        self.logger.info(f"{current_time}: End cleaning rules for completed element {element.id}")
            
            if not do_cleaning:
                current_time = format_timestamp(time.time())
                self.logger.info(f"{current_time} No cleaning happened: There are no completed workqueue elements or rules already cleaned")    
            
            #current_time = format_timestamp(time.time())
            #self.logger.info(f"{current_time}: {self.__class__.__name__} executed in {(time.time() - tStart):.3f} secs.")
            #tmp = rulesToClean['RulesToClean'][rulesToClean['PlineMarkers'][-1]]
            #ids = ''
            #for rid in tmp:
            #    ids += rid + ', '
            #    rulesToClean['CleanupStatus']['Current'].append({'RuleID': rid, 'Status': 'Pending'})
            #self.logger.info('Rules to be cleaned: %s', ids)
            #return rulesToClean
            #return self.msRuleCleaner.cleanRucioRules(rulesToClean)

        else:
            current_time = format_timestamp(time.time())
            self.logger.info(f"{current_time} No elements found in GlobalQueue")
        
        current_time = format_timestamp(time.time())
        self.logger.info(f"{current_time} {self.__class__.__name__} executed in {(time.time() - tStart):.3f} secs.")
        return do_cleaning